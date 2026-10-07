package com.github.h3yow.columnarcompactionstorejava.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
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
 *   <li>{@link #openSnapshot()} pins the current data version: the returned
 *       {@link VersionSnapshot} reads exactly that version until closed,
 *       regardless of later writes, compactions or retention cleanups.
 *       Concurrent snapshots are bounded by count and pinned bytes;
 *       over-limit opens are rejected with
 *       {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED}.</li>
 *   <li>{@link #enforceRetention(RetentionPolicy)} expires old segments by
 *       age or version count, atomically dropping them from the manifest
 *       before reclaiming files. It is serialized with compaction, safe to
 *       interrupt and retry, idempotent on repetition, and never deletes a
 *       segment still pinned by an open snapshot.</li>
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
    private final SnapshotRegistry snapshotRegistry;
    private final LongSupplier clock;
    private final ReentrantLock manifestLock = new ReentrantLock();
    private final ReentrantLock compactionLock = new ReentrantLock();
    private final AtomicLong segmentSeq = new AtomicLong();
    /** Segments dropped from the manifest but still pinned by a snapshot;
     *  reclaimed as soon as the last pin goes away. */
    private final Set<String> pendingReclaim = ConcurrentHashMap.newKeySet();

    private volatile Manifest manifest;
    private volatile boolean closed;
    /** Test hook: invoked mid-compaction, after the merged segment is durable
     *  but before the manifest swap, to simulate a crash. */
    volatile Runnable compactionCrashHook = () -> { };
    /** Test hook: invoked mid-retention-cleanup, after the manifest swap but
     *  before expired segment files are reclaimed, to simulate a crash. */
    volatile Runnable retentionCrashHook = () -> { };

    private ColumnarStore(Path dir, ResourceLimiter limiter, long resourceWaitMillis,
                          SnapshotRegistry.SnapshotLimits snapshotLimits,
                          LongSupplier clock, Manifest manifest) {
        this.dir = dir;
        this.limiter = limiter;
        this.resourceWaitMillis = resourceWaitMillis;
        this.snapshotRegistry = new SnapshotRegistry(dir, snapshotLimits);
        this.clock = clock;
        this.manifest = manifest;
    }

    /** Opens (or creates) the store rooted at {@code dir}. */
    public static ColumnarStore open(Path dir, Schema initialSchema,
                                     ResourceLimiter limiter, long resourceWaitMillis) {
        return open(dir, initialSchema, limiter, resourceWaitMillis,
                SnapshotRegistry.SnapshotLimits.defaults(), System::currentTimeMillis);
    }

    /**
     * Opens (or creates) the store with explicit snapshot limits and clock.
     * The clock only stamps segment creation times for retention; tests can
     * inject a controllable one.
     */
    public static ColumnarStore open(Path dir, Schema initialSchema,
                                     ResourceLimiter limiter, long resourceWaitMillis,
                                     SnapshotRegistry.SnapshotLimits snapshotLimits,
                                     LongSupplier clock) {
        Objects.requireNonNull(initialSchema, "initialSchema");
        Objects.requireNonNull(snapshotLimits, "snapshotLimits");
        Objects.requireNonNull(clock, "clock");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH,
                    "cannot create store dir " + dir + ": " + e.getMessage(), e);
        }
        Manifest manifest = Manifest.load(dir, clock);
        if (manifest == null) {
            manifest = Manifest.empty(initialSchema);
            manifest.save(dir);
        }
        ColumnarStore store = new ColumnarStore(dir, limiter, resourceWaitMillis,
                snapshotLimits, clock, manifest);
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
            // 2. atomic manifest swap makes the batch visible as one unit;
            //    re-read under the lock so a concurrent compaction/cleanup
            //    can never be clobbered by a stale segment list
            mutateManifest(cur -> {
                List<Manifest.SegmentRef> segments = new ArrayList<>(cur.segmentRefs());
                segments.add(new Manifest.SegmentRef(segmentName, clock.getAsLong()));
                return cur.withSegments(segments, cur.compactionGeneration());
            });
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
    static Map<String, Object> fillDefaults(Map<String, Object> row, Schema schema) {
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
            // the merged segment inherits the oldest input's creation time so
            // age-based retention still sees the true data age
            long mergedCreatedAt = before.segmentRefs().stream()
                    .mapToLong(Manifest.SegmentRef::createdAtMillis)
                    .min().orElse(clock.getAsLong());
            // splice the merged segment into the *current* manifest under the
            // lock, so batches written while we merged are preserved
            Set<String> mergedInputs = new HashSet<>(live);
            mutateManifest(cur -> {
                List<Manifest.SegmentRef> segs = new ArrayList<>();
                boolean inserted = false;
                for (Manifest.SegmentRef ref : cur.segmentRefs()) {
                    if (mergedInputs.contains(ref.name())) {
                        if (!inserted) {
                            segs.add(new Manifest.SegmentRef(mergedName, mergedCreatedAt));
                            inserted = true;
                        }
                    } else {
                        segs.add(ref);
                    }
                }
                if (!inserted) {
                    segs.add(new Manifest.SegmentRef(mergedName, mergedCreatedAt));
                }
                // record pending reclaim before the swap is durable (see
                // enforceRetention); drain only deletes non-live segments
                pendingReclaim.addAll(mergedInputs);
                return cur.withSegments(segs, cur.compactionGeneration() + 1);
            });
            // reclaim old segments only after the new manifest is durable;
            // segments still pinned by a snapshot are deferred, not deleted
            reclaimSegments(live);
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

    private Manifest mutateManifest(Function<Manifest, Manifest> fn) {
        manifestLock.lock();
        try {
            Manifest cur = manifest;
            Manifest updated = fn.apply(cur);
            if (updated != cur) {
                updated.save(dir);
                manifest = updated;
            }
            return manifest;
        } finally {
            manifestLock.unlock();
        }
    }

    // -------------------------------------------------------------- snapshot

    /**
     * Opens a pinned version snapshot at the current data version. The
     * returned handle reads exactly the logical data visible right now,
     * unaffected by later writes, compactions or retention cleanups, until
     * {@link VersionSnapshot#close()}.
     *
     * <p>Opening waits up to the store's resource wait for a snapshot slot;
     * if the snapshot-count or pinned-bytes limit cannot be satisfied it is
     * rejected with {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED}.
     */
    public VersionSnapshot openSnapshot() {
        ensureOpen();
        // manifest read + pin acquisition are atomic w.r.t. segment
        // reclamation, so a pinned segment can never be deleted under us
        manifestLock.lock();
        try {
            Manifest pinned = manifest;
            SnapshotRegistry.Pin pin = snapshotRegistry.acquire(pinned, resourceWaitMillis);
            return new VersionSnapshot(snapshotRegistry, pin, dir, limiter,
                    resourceWaitMillis, () -> !closed, this::drainPendingReclaim);
        } finally {
            manifestLock.unlock();
        }
    }

    /** Number of currently open version snapshots. */
    public int openSnapshotCount() {
        return snapshotRegistry.openCount();
    }

    /** Bytes currently pinned on disk by open snapshots. */
    public long snapshotPinnedBytes() {
        return snapshotRegistry.pinnedBytes();
    }

    // ------------------------------------------------------------- retention

    /** Outcome of one {@link #enforceRetention(RetentionPolicy)} call. */
    public record RetentionResult(int expiredSegments, int reclaimedSegments,
                                  int deferredToSnapshots) {
    }

    /**
     * Applies a retention policy: live segments expired by {@code policy}
     * are removed from the manifest (so new reads no longer see them) and
     * their files are deleted, unless an open snapshot still pins them —
     * those are reclaimed automatically when the last pinning snapshot
     * closes.
     *
     * <p>Serialized with compaction and other cleanups, safe to interrupt
     * (the manifest swap is atomic; a crash afterwards only leaves files to
     * be reclaimed by a retry) and idempotent (a retry finds the same
     * segments already gone from the manifest and does nothing).
     */
    public RetentionResult enforceRetention(RetentionPolicy policy) {
        ensureOpen();
        Objects.requireNonNull(policy, "policy");
        compactionLock.lock();
        try {
            List<Manifest.SegmentRef> expired = new ArrayList<>();
            mutateManifest(cur -> {
                expired.addAll(selectExpired(cur.segmentRefs(), policy));
                if (expired.isEmpty()) {
                    return cur; // idempotent no-op: no version bump, no rewrite
                }
                Set<String> expiredNames = new HashSet<>();
                for (Manifest.SegmentRef ref : expired) {
                    expiredNames.add(ref.name());
                }
                List<Manifest.SegmentRef> kept = new ArrayList<>();
                for (Manifest.SegmentRef ref : cur.segmentRefs()) {
                    if (!expiredNames.contains(ref.name())) {
                        kept.add(ref);
                    }
                }
                // record the pending reclaim *before* the swap becomes
                // durable, so an interrupted cleanup can be completed by any
                // later drain (retry, compaction or snapshot release)
                pendingReclaim.addAll(expiredNames);
                return cur.withSegments(kept, cur.compactionGeneration());
            });
            if (expired.isEmpty()) {
                // still drain: a previously interrupted cleanup may have left
                // pending segments behind, and a retry must complete them
                drainPendingReclaim();
                return new RetentionResult(0, 0, 0);
            }
            // crash point: manifest no longer references the expired segments;
            // a crash here only delays file reclamation, never loses live data
            retentionCrashHook.run();
            List<String> names = new ArrayList<>(expired.size());
            for (Manifest.SegmentRef ref : expired) {
                names.add(ref.name());
            }
            drainPendingReclaim();
            int deferred = 0;
            for (String name : names) {
                if (pendingReclaim.contains(name)) {
                    deferred++;
                }
            }
            return new RetentionResult(expired.size(), expired.size() - deferred, deferred);
        } finally {
            compactionLock.unlock();
        }
    }

    /** Computes which live segments the policy expires. */
    private List<Manifest.SegmentRef> selectExpired(List<Manifest.SegmentRef> live,
                                                    RetentionPolicy policy) {
        List<Manifest.SegmentRef> expired = new ArrayList<>();
        if (policy instanceof RetentionPolicy.MaxAge maxAge) {
            long cutoff = clock.getAsLong() - maxAge.maxAge().toMillis();
            for (Manifest.SegmentRef ref : live) {
                if (ref.createdAtMillis() <= cutoff) {
                    expired.add(ref);
                }
            }
        } else if (policy instanceof RetentionPolicy.KeepLastVersions keepLast) {
            int excess = live.size() - keepLast.keep();
            for (int i = 0; i < excess; i++) {
                expired.add(live.get(i));
            }
        }
        return expired;
    }

    /**
     * Drops {@code names} from circulation: deletes every file not pinned by
     * a snapshot right away and defers the rest until the last pin closes.
     */
    private void reclaimSegments(List<String> names) {
        pendingReclaim.addAll(names);
        drainPendingReclaim();
    }

    /**
     * Deletes every pending segment that is neither referenced by the live
     * manifest nor pinned by a snapshot. Serialized with
     * {@link #openSnapshot()} on the manifest lock so the checks and the
     * delete are atomic w.r.t. pin acquisition; a segment still referenced
     * by the manifest (crash before the swap) is never touched.
     */
    private void drainPendingReclaim() {
        manifestLock.lock();
        try {
            Set<String> live = new HashSet<>(manifest.liveSegments());
            List<String> done = new ArrayList<>();
            for (String name : pendingReclaim) {
                if (!live.contains(name) && !snapshotRegistry.isPinned(name)) {
                    deleteQuietly(dir.resolve(name));
                    done.add(name);
                }
            }
            pendingReclaim.removeAll(done);
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
