package com.github.h3yow.columnarcompactionstorejava.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Local-disk columnar store with batch writes, snapshot reads and compaction.
 *
 * <p>Consistency model:
 * <ul>
 *   <li>Segment files are immutable and written atomically (tmp + fsync +
 *       rename); the manifest is swapped atomically the same way. A reader
 *       therefore always sees a complete snapshot — never a half-written
 *       segment or a half-compacted set.</li>
 *   <li>A failed compaction leaves the previous manifest and segments
 *       untouched; orphaned files are cleaned up on next open.</li>
 *   <li>Compaction is serialized and idempotent: once the live set is a
 *       single segment, further triggers are no-ops, so repeated triggers
 *       never duplicate rows or counts.</li>
 *   <li>Writes and compaction acquire memory/file permits from a
 *       {@link ResourceLimiter}; over-budget operations are rejected with
 *       {@link ErrorCode#RESOURCE_LIMIT_EXCEEDED}.</li>
 * </ul>
 */
public final class ColumnarStore implements AutoCloseable {

    /** An immutable view of the store: schema plus all rows in write order. */
    public record Snapshot(Schema schema, List<Map<String, Object>> rows) {
    }

    private static final Pattern SEGMENT_NAME = Pattern.compile("seg-(\\d+)\\.ccs");
    private static final long ESTIMATED_ROW_OVERHEAD_BYTES = 64;

    private final Path dir;
    private final ResourceLimiter limiter;
    private final long resourceWaitMillis;
    private final ReentrantLock manifestLock = new ReentrantLock();
    private final ReentrantLock compactionLock = new ReentrantLock();
    private final AtomicLong segmentSeq = new AtomicLong();

    private volatile Manifest manifest;
    private volatile boolean closed;
    /** Test hook: invoked mid-compaction, after the merged segment is durable
     *  but before the manifest swap, to simulate a crash. */
    volatile Runnable compactionCrashHook = () -> { };

    private ColumnarStore(Path dir, ResourceLimiter limiter, long resourceWaitMillis,
                          Manifest manifest) {
        this.dir = dir;
        this.limiter = limiter;
        this.resourceWaitMillis = resourceWaitMillis;
        this.manifest = manifest;
    }

    /** Opens (or creates) the store rooted at {@code dir}. */
    public static ColumnarStore open(Path dir, Schema initialSchema,
                                     ResourceLimiter limiter, long resourceWaitMillis) {
        Objects.requireNonNull(initialSchema, "initialSchema");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH,
                    "cannot create store dir " + dir + ": " + e.getMessage(), e);
        }
        Manifest manifest = Manifest.load(dir);
        if (manifest == null) {
            manifest = new Manifest(initialSchema, List.of(), 0);
            manifest.save(dir);
        }
        ColumnarStore store = new ColumnarStore(dir, limiter, resourceWaitMillis, manifest);
        store.cleanupOrphans();
        return store;
    }

    public static ColumnarStore open(Path dir, Schema initialSchema, ResourceLimiter limiter) {
        return open(dir, initialSchema, limiter, 50);
    }

    // ------------------------------------------------------------------ write

    /**
     * Validates and durably appends a batch. The whole batch is rejected —
     * never partially applied — with a distinguishable {@link ErrorCode}:
     * {@link ErrorCode#MALFORMED_BATCH}, {@link ErrorCode#UNKNOWN_FIELD},
     * {@link ErrorCode#MISSING_FIELD} or {@link ErrorCode#TYPE_MISMATCH}.
     */
    public void write(Batch batch) {
        ensureOpen();
        Manifest current = manifest;
        Schema schema = current.schema();
        validate(batch, schema);

        long estimatedBytes = estimateBytes(batch.rows());
        String segmentName = nextSegmentName();
        Path segmentPath = dir.resolve(segmentName);
        try (ResourceLimiter.MemoryGrant mem = limiter.acquireMemory(estimatedBytes, resourceWaitMillis);
             ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
            // 1. segment becomes durable but is not yet referenced by any manifest
            SegmentFormat.write(segmentPath, schema, batch.rows());
            // 2. atomic manifest swap makes the batch visible as one unit
            List<String> segments = new ArrayList<>(current.liveSegments());
            segments.add(segmentName);
            swapManifest(current.withSegments(segments, current.compactionGeneration()));
        } catch (StoreException e) {
            deleteQuietly(segmentPath);
            throw e;
        }
    }

    private void validate(Batch batch, Schema schema) {
        if (batch == null || batch.rows() == null) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH, "batch or batch.rows is null");
        }
        if (batch.rows().isEmpty()) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH, "batch contains no rows");
        }
        int rowIndex = 0;
        for (Map<String, Object> row : batch.rows()) {
            if (row == null) {
                throw new StoreException(ErrorCode.MALFORMED_BATCH,
                        "row " + rowIndex + " is null");
            }
            for (String key : row.keySet()) {
                if (!schema.contains(key)) {
                    throw new StoreException(ErrorCode.UNKNOWN_FIELD,
                            "row " + rowIndex + ": field '" + key + "' is not in schema " + schema);
                }
            }
            for (ColumnDef col : schema.columns()) {
                Object value = row.get(col.name());
                if (value == null) {
                    throw new StoreException(ErrorCode.MISSING_FIELD,
                            "row " + rowIndex + ": required field '" + col.name() + "' is absent or null");
                }
                if (!typeMatches(col.type(), value)) {
                    throw new StoreException(ErrorCode.TYPE_MISMATCH,
                            "row " + rowIndex + ": field '" + col.name() + "' expects "
                                    + col.type() + " but got " + value.getClass().getSimpleName()
                                    + " (" + value + ")");
                }
            }
            rowIndex++;
        }
    }

    private static boolean typeMatches(ColumnType type, Object value) {
        return switch (type) {
            case INT -> value instanceof Integer;
            case LONG -> value instanceof Long;
            case DOUBLE -> value instanceof Double;
            case BOOL -> value instanceof Boolean;
            case STRING -> value instanceof String;
        };
    }

    // ------------------------------------------------------------------- read

    /**
     * Returns a consistent snapshot of all live rows. Concurrent compaction
     * can never surface a half-merged state: the snapshot pins one manifest,
     * and if a pinned segment is concurrently reclaimed the read transparently
     * retries on the newer manifest.
     */
    public Snapshot readSnapshot() {
        ensureOpen();
        StoreException lastError = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            Manifest pinned = manifest;
            Schema schema = pinned.schema();
            List<Map<String, Object>> rows = new ArrayList<>();
            try {
                for (String segmentName : pinned.liveSegments()) {
                    SegmentFormat.SegmentData data;
                    try (ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                        data = SegmentFormat.read(dir.resolve(segmentName));
                    }
                    for (Map<String, Object> row : data.rows()) {
                        rows.add(fillDefaults(row, schema));
                    }
                }
                return new Snapshot(schema, List.copyOf(rows));
            } catch (StoreException e) {
                if (e.getCause() instanceof NoSuchFileException) {
                    // a compaction reclaimed a pinned segment mid-read: retry on new manifest
                    lastError = e;
                    continue;
                }
                throw e;
            }
        }
        throw new StoreException(ErrorCode.CORRUPT_SEGMENT,
                "could not obtain a stable snapshot after retries", lastError);
    }

    /** Fills deterministic defaults for columns added after the row was written. */
    private static Map<String, Object> fillDefaults(Map<String, Object> row, Schema schema) {
        Map<String, Object> full = new java.util.LinkedHashMap<>(row);
        for (ColumnDef col : schema.columns()) {
            full.putIfAbsent(col.name(), Schema.defaultValueOf(col.type()));
        }
        return full;
    }

    // --------------------------------------------------------------- compact

    /**
     * Merges all live segments into one. Serialized and idempotent: a trigger
     * arriving while another compaction runs waits for it and then finds
     * nothing to do; a trigger after completion is a no-op. A crash before
     * the manifest swap leaves the old snapshot fully intact.
     *
     * @return {@code true} if a merge actually happened
     */
    public boolean compact() {
        ensureOpen();
        compactionLock.lock();
        try {
            Manifest before = manifest;
            List<String> live = before.liveSegments();
            if (live.size() <= 1) {
                return false; // idempotent: nothing (or already) compacted
            }
            Schema schema = before.schema();
            // merge rows of all live segments in order
            List<Map<String, Object>> merged = new ArrayList<>();
            for (String segmentName : live) {
                SegmentFormat.SegmentData data;
                try (ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                    data = SegmentFormat.read(dir.resolve(segmentName));
                }
                for (Map<String, Object> row : data.rows()) {
                    merged.add(fillDefaults(row, schema));
                }
            }
            long estimatedBytes = estimateBytes(merged);
            String mergedName = nextSegmentName();
            Path mergedPath = dir.resolve(mergedName);
            try (ResourceLimiter.MemoryGrant mem = limiter.acquireMemory(estimatedBytes, resourceWaitMillis);
                 ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                SegmentFormat.write(mergedPath, schema, merged);
            }
            // crash point: merged segment is durable but invisible; old state intact
            compactionCrashHook.run();
            swapManifest(before.withSegments(List.of(mergedName),
                    before.compactionGeneration() + 1));
            // reclaim old segments only after the new manifest is durable
            for (String old : live) {
                deleteQuietly(dir.resolve(old));
            }
            return true;
        } finally {
            compactionLock.unlock();
        }
    }

    // -------------------------------------------------------------- schema

    /** Appends a column; pre-existing rows read back with the type default. */
    public void addColumn(String name, ColumnType type) {
        ensureOpen();
        manifestLock.lock();
        try {
            Manifest updated = manifest.withSchema(manifest.schema().addColumn(name, type));
            updated.save(dir);
            manifest = updated;
        } finally {
            manifestLock.unlock();
        }
    }

    /**
     * Validates and applies a proposed schema. Only pure appends are legal;
     * rename, drop or retype are rejected with
     * {@link ErrorCode#SCHEMA_INCOMPATIBLE} so readers can never silently
     * misalign columns.
     */
    public void evolveTo(Schema proposed) {
        ensureOpen();
        manifestLock.lock();
        try {
            manifest.schema().assertCompatibleSuccessor(proposed);
            if (proposed.columnCount() != manifest.schema().columnCount()) {
                Manifest updated = manifest.withSchema(proposed);
                updated.save(dir);
                manifest = updated;
            }
        } finally {
            manifestLock.unlock();
        }
    }

    public Manifest currentManifest() {
        return manifest;
    }

    // -------------------------------------------------------------- internals

    private void swapManifest(Manifest updated) {
        manifestLock.lock();
        try {
            updated.save(dir);
            manifest = updated;
        } finally {
            manifestLock.unlock();
        }
    }

    private String nextSegmentName() {
        return String.format("seg-%06d.ccs", segmentSeq.incrementAndGet());
    }

    private long estimateBytes(List<Map<String, Object>> rows) {
        long bytes = 0;
        for (Map<String, Object> row : rows) {
            bytes += ESTIMATED_ROW_OVERHEAD_BYTES;
            for (Object value : row.values()) {
                if (value instanceof String s) {
                    bytes += 2L * s.length() + 16;
                } else {
                    bytes += 16;
                }
            }
        }
        return Math.max(bytes, 1);
    }

    private void ensureOpen() {
        if (closed) {
            throw new StoreException(ErrorCode.STORE_CLOSED, "store is closed");
        }
    }

    /** Removes tmp files and segments not referenced by the manifest. */
    private void cleanupOrphans() {
        try (Stream<Path> files = Files.list(dir)) {
            List<String> live = manifest.liveSegments();
            files.forEach(p -> {
                String name = p.getFileName().toString();
                Matcher m = SEGMENT_NAME.matcher(name);
                if (m.matches()) {
                    segmentSeq.updateAndGet(cur ->
                            Math.max(cur, Long.parseLong(m.group(1))));
                    if (!live.contains(name)) {
                        deleteQuietly(p);
                    }
                } else if (name.endsWith(SegmentFormat.TMP_SUFFIX)) {
                    deleteQuietly(p);
                }
            });
        } catch (IOException e) {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT,
                    "cannot list store dir: " + e.getMessage(), e);
        }
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // best effort; orphan will be cleaned on next open
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
