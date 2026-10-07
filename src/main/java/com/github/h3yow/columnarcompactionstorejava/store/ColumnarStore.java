package com.github.h3yow.columnarcompactionstorejava.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Local-disk columnar store with batch writes, snapshot reads, compaction,
 * explicit version snapshots and retention-based expiration.
 *
 * <p>Consistency model:
 * <ul>
 *   <li>Segment files are immutable and written atomically (tmp + fsync +
 *       rename); the manifest is swapped atomically the same way. A reader
 *       therefore always sees a complete snapshot — never a half-written
 *       segment or a half-compacted set.</li>
 *   <li>Every successful write/compaction appends one immutable
 *       {@link VersionInfo} to the manifest's version catalog. An explicitly
 *       opened {@link VersionSnapshot} pins that version's segments; writes,
 *       compactions and retention cleanup cannot alter or remove them until
 *       the snapshot is released.</li>
 *   <li>Retention trims the catalog first (atomic manifest commit), then
 *       reclaims segment files that no retained version, live set or open
 *       snapshot references. Interruption or failure after the commit is
 *       harmless: retry reclaims the leftovers and nothing can resurrect
 *       expired data.</li>
 *   <li>Writes/compactions acquire permits from a {@link ResourceLimiter};
 *       snapshot count and pinned bytes are bounded by {@link SnapshotLimits}
 *       — over-budget calls wait briefly and are then rejected with
 *       distinguishable {@link ErrorCode}s.</li>
 * </ul>
 */
public final class ColumnarStore implements AutoCloseable {

    /** An immutable view of the store: schema plus all rows in write order. */
    public record Snapshot(Schema schema, List<Map<String, Object>> rows) {
    }

    /** Outcome of one retention pass. */
    public record RetentionResult(int expiredVersions, int deletedFiles,
                                  long deletedBytes) {
    }

    private static final Pattern SEGMENT_NAME = Pattern.compile("seg-(\\d+)\\.ccs");
    private static final long ESTIMATED_ROW_OVERHEAD_BYTES = 64;
    private static final SnapshotLimits DEFAULT_SNAPSHOT_LIMITS =
            new SnapshotLimits(16, 256L << 20);

    private final Path dir;
    private final ResourceLimiter limiter;
    private final long resourceWaitMillis;
    private final Clock clock;
    private volatile RetentionPolicy retention;
    private final SnapshotRegistry snapshots;
    private final ReentrantLock manifestLock = new ReentrantLock();
    /** Serializes every topology change: writes, compactions, cleanup and snapshot opens. */
    private final ReentrantLock lifecycleLock = new ReentrantLock(true);
    private final ReentrantLock compactionLock = new ReentrantLock();
    private final AtomicLong segmentSeq = new AtomicLong();
    private final AtomicLong versionSeq = new AtomicLong();
    private final AtomicLong snapshotSeq = new AtomicLong();

    private volatile Manifest manifest;
    private volatile boolean closed;
    /** Test hook: invoked mid-compaction, after the merged segment is durable
     *  but before the manifest swap, to simulate a crash. */
    volatile Runnable compactionCrashHook = () -> { };
    /** Test hook: invoked at the start of a retention pass before its commit. */
    volatile Runnable retentionCrashHook = () -> { };
    /** Test hook: invoked per file during reclaim, so interrupts land mid-deletion. */
    volatile Runnable reclaimStepHook = () -> { };

    private ColumnarStore(Path dir, ResourceLimiter limiter, long resourceWaitMillis,
                          Clock clock, RetentionPolicy retention,
                          SnapshotLimits snapshotLimits, Manifest manifest) {
        this.dir = dir;
        this.limiter = limiter;
        this.resourceWaitMillis = resourceWaitMillis;
        this.clock = clock;
        this.retention = retention;
        this.snapshots = new SnapshotRegistry(snapshotLimits);
        this.manifest = manifest;
    }

    /** Opens (or creates) the store rooted at {@code dir} with default retention. */
    public static ColumnarStore open(Path dir, Schema initialSchema,
                                     ResourceLimiter limiter, long resourceWaitMillis) {
        return open(dir, initialSchema, limiter, resourceWaitMillis,
                RetentionPolicy.currentOnly(), DEFAULT_SNAPSHOT_LIMITS, Clock.systemUTC());
    }

    /** Opens (or creates) the store with explicit retention and snapshot caps. */
    public static ColumnarStore open(Path dir, Schema initialSchema, ResourceLimiter limiter,
                                     long resourceWaitMillis, RetentionPolicy retention,
                                     SnapshotLimits snapshotLimits, Clock clock) {
        Objects.requireNonNull(initialSchema, "initialSchema");
        Objects.requireNonNull(retention, "retention");
        Objects.requireNonNull(snapshotLimits, "snapshotLimits");
        Objects.requireNonNull(clock, "clock");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH,
                    "cannot create store dir " + dir + ": " + e.getMessage(), e);
        }
        Manifest loaded = Manifest.load(dir);
        if (loaded == null) {
            loaded = new Manifest(initialSchema, List.of(), 0);
            loaded.save(dir);
        } else if (loaded.history().isEmpty() && !loaded.liveSegments().isEmpty()) {
            // One-time upgrade of a pre-versions manifest: the current live set
            // becomes version 1, timestamped with the manifest file's mtime.
            long created = System.currentTimeMillis();
            try {
                created = Files.getLastModifiedTime(dir.resolve(Manifest.FILE_NAME)).toMillis();
            } catch (IOException ignored) {
                // best effort: fall back to now
            }
            VersionInfo synthesized = new VersionInfo(1, created,
                    loaded.compactionGeneration(), loaded.schema().version(),
                    loaded.schema().columnCount(), loaded.liveSegments());
            loaded = loaded.withHistory(List.of(synthesized));
            loaded.save(dir);
        }
        ColumnarStore store = new ColumnarStore(dir, limiter, resourceWaitMillis, clock,
                retention, snapshotLimits, loaded);
        long maxVersion = 0;
        for (VersionInfo v : loaded.history()) {
            maxVersion = Math.max(maxVersion, v.versionId());
        }
        store.versionSeq.set(maxVersion);
        store.cleanupOrphans();
        return store;
    }

    public static ColumnarStore open(Path dir, Schema initialSchema, ResourceLimiter limiter) {
        return open(dir, initialSchema, limiter, 50);
    }

    // ------------------------------------------------------------------ write

    /**
     * Validates and durably appends a batch, atomically creating a new store
     * version. The whole batch is rejected — never partially applied — with a
     * distinguishable {@link ErrorCode}.
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
            // 2. atomic manifest swap publishes the batch AND its version as one unit
            lifecycleLock.lock();
            try {
                current = manifest;
                List<String> segmentNames = new ArrayList<>(current.liveSegments());
                segmentNames.add(segmentName);
                long versionId = nextVersionId(current);
                VersionInfo version = new VersionInfo(versionId, clock.millis(),
                        current.compactionGeneration(), schema.version(),
                        schema.columnCount(), List.copyOf(segmentNames));
                commitManifest(current.withNewVersion(segmentNames,
                        current.compactionGeneration(), version));
                // 3. best-effort space reclamation under the retention window
                reclaimBestEffort();
            } finally {
                lifecycleLock.unlock();
            }
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
     * Returns a consistent snapshot of all current live rows.
     */
    public Snapshot readSnapshot() {
        ensureOpen();
        StoreException lastError = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            Manifest pinned = manifest;
            Schema schema = pinned.schema();
            List<Map<String, Object>> rows = new ArrayList<>();
            try {
                readSegmentsInto(pinned.liveSegments(), schema, rows);
                return new Snapshot(schema, List.copyOf(rows));
            } catch (StoreException e) {
                if (e.getCause() instanceof NoSuchFileException
                        || e.code() == ErrorCode.SNAPSHOT_DATA_UNAVAILABLE) {
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

    // ------------------------------------------------------ version snapshots

    /** Opens a snapshot of the current head version. */
    public VersionSnapshot openSnapshot() {
        ensureOpen();
        Manifest current = manifest;
        VersionInfo head = current.headVersion();
        if (head == null) {
            throw new StoreException(ErrorCode.SNAPSHOT_VERSION_EXPIRED,
                    "store has no versions yet (nothing written)");
        }
        return openVersion(current, head.versionId());
    }

    /**
     * Opens a snapshot of an explicit historical version.
     *
     * @throws StoreException {@link ErrorCode#SNAPSHOT_VERSION_EXPIRED} if the
     *         version left the retention window (or never existed),
     *         {@link ErrorCode#SNAPSHOT_DATA_UNAVAILABLE} if its files are
     *         missing, {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED} /
     *         {@link ErrorCode#SNAPSHOT_BYTES_LIMIT_EXCEEDED} on cap overflow
     *         after waiting.
     */
    public VersionSnapshot openSnapshot(long versionId) {
        ensureOpen();
        return openVersion(manifest, versionId);
    }

    private VersionSnapshot openVersion(Manifest current, long versionId) {
        lifecycleLock.lock();
        try {
            VersionInfo version = current.version(versionId);
            if (version == null || !manifest.history().contains(version)) {
                throw new StoreException(ErrorCode.SNAPSHOT_VERSION_EXPIRED,
                        "version " + versionId + " is not within the retention window"
                                + " (latest retained: " + describeLatest() + ")");
            }
            Map<String, Long> sizes = new java.util.LinkedHashMap<>();
            for (String name : version.segments()) {
                Path path = dir.resolve(name);
                long size;
                try {
                    if (!Files.exists(path)) {
                        throw new StoreException(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE,
                                "backing segment " + name + " for version " + versionId
                                        + " is missing; consistency cannot be guaranteed");
                    }
                    size = Files.size(path);
                } catch (NoSuchFileException e) {
                    throw new StoreException(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE,
                            "backing segment " + name + " for version " + versionId
                                    + " vanished before open", e);
                } catch (IOException e) {
                    throw new StoreException(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE,
                            "cannot stat backing segment " + name + " for version "
                                    + versionId + ": " + e.getMessage(), e);
                }
                sizes.put(name, size);
            }
            VersionSnapshot snapshot =
                    new VersionSnapshot(snapshotSeq.incrementAndGet(), version, this);
            // attach while holding the lifecycle lock: cleanup cannot delete the
            // backing files between the existence check and the pin taking effect
            snapshots.attach(snapshot, sizes, resourceWaitMillis);
            return snapshot;
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** Called by {@link VersionSnapshot#read()}; never observes later data. */
    Snapshot readVersion(VersionSnapshot handle) {
        ensureOpen();
        VersionInfo version = handle.versionInfo();
        // append-only evolution: the schema at that version is a prefix of today's
        Schema currentSchema = manifest.schema();
        Schema schemaAtVersion = Schema.of(version.schemaVersion(),
                currentSchema.columns().subList(0,
                        Math.min(version.columnCount(), currentSchema.columnCount())));
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            readSegmentsInto(version.segments(), schemaAtVersion, rows);
        } catch (StoreException e) {
            if (e.getCause() instanceof NoSuchFileException
                    || e.code() == ErrorCode.SNAPSHOT_DATA_UNAVAILABLE) {
                throw unavailable(version, e);
            }
            throw e;
        }
        return new Snapshot(schemaAtVersion, List.copyOf(rows));
    }

    private static StoreException unavailable(VersionInfo version, Throwable cause) {
        return new StoreException(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE,
                "snapshot of version " + version.versionId()
                        + " can no longer be served consistently (backing data removed)",
                cause);
    }

    /** Called by {@link VersionSnapshot#close()}; idempotent. */
    void releaseSnapshot(VersionSnapshot handle) {
        snapshots.release(handle);
        handle.markClosed();
    }

    /** Catalog of currently retained versions, oldest first. */
    public List<VersionInfo> versions() {
        ensureOpen();
        return manifest.history();
    }

    public int activeSnapshotCount() {
        return snapshots.activeCount();
    }

    public long pinnedBytes() {
        return snapshots.pinnedUnionBytes();
    }

    public RetentionPolicy retentionPolicy() {
        return retention;
    }

    /** Replaces the retention policy at runtime (package-visible; tests/admins). */
    void replaceRetentionPolicy(RetentionPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        this.retention = policy;
    }

    // --------------------------------------------------------------- compact

    /**
     * Merges all live segments into one and publishes the merged state as a
     * new version. Old segments are not deleted here: they remain until
     * retention expires their versions (and are always kept while pinned by an
     * open snapshot). With the default current-only retention they are
     * reclaimed immediately after the commit, preserving historical behavior.
     *
     * @return {@code true} if a merge actually happened
     */
    public boolean compact() {
        ensureOpen();
        compactionLock.lock();
        try {
            lifecycleLock.lock();
            try {
                Manifest before = manifest;
                List<String> live = before.liveSegments();
                if (live.size() <= 1) {
                    return false; // idempotent: nothing (or already) compacted
                }
                Schema schema = before.schema();
                List<Map<String, Object>> merged = new ArrayList<>();
                readSegmentsInto(live, schema, merged);
                long estimatedBytes = estimateBytes(merged);
                String mergedName = nextSegmentName();
                Path mergedPath = dir.resolve(mergedName);
                try (ResourceLimiter.MemoryGrant mem = limiter.acquireMemory(estimatedBytes, resourceWaitMillis);
                     ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                    SegmentFormat.write(mergedPath, schema, merged);
                }
                // crash point: merged segment is durable but invisible; old state intact
                compactionCrashHook.run();
                long newGeneration = before.compactionGeneration() + 1;
                long versionId = nextVersionId(before);
                VersionInfo version = new VersionInfo(versionId, clock.millis(),
                        newGeneration, schema.version(), schema.columnCount(),
                        List.of(mergedName));
                commitManifest(before.withNewVersion(List.of(mergedName),
                        newGeneration, version));
                reclaimBestEffort();
                return true;
            } finally {
                lifecycleLock.unlock();
            }
        } finally {
            compactionLock.unlock();
        }
    }

    // -------------------------------------------------------------- retention

    /**
     * Applies the retention policy: expires old catalog versions and truly
     * deletes every segment file that no retained version, live set or open
     * snapshot references (space is returned to the OS).
     *
     * <p>Safe to interrupt, fail and repeat: the catalog trim is one atomic
     * commit; file deletion afterwards is pure garbage collection whose
     * leftovers are reclaimed by the next pass. Expired data never reappears,
     * because a file dropped from the catalog can never re-enter it.
     *
     * @throws StoreException {@link ErrorCode#CLEANUP_INTERRUPTED} if the
     *         caller's thread was interrupted mid-deletion (a retry completes
     *         the remaining reclamation)
     */
    public RetentionResult enforceRetention() {
        ensureOpen();
        lifecycleLock.lock();
        try {
            retentionCrashHook.run();
            Manifest current = manifest;
            long now = clock.millis();
            List<VersionInfo> retained = retention.retainedVersions(current.history(), now);
            int expired = current.history().size() - retained.size();
            if (expired > 0) {
                commitManifest(current.withHistory(retained));
            }
            return reclaimUnreferencedFiles(expired);
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** Best-effort reclaim used after committed writes/compactions. */
    private void reclaimBestEffort() {
        try {
            Manifest current = manifest;
            List<VersionInfo> retained = retention.retainedVersions(
                    current.history(), clock.millis());
            int expired = current.history().size() - retained.size();
            if (expired > 0) {
                commitManifest(current.withHistory(retained));
            }
            reclaimUnreferencedFiles(expired);
        } catch (StoreException e) {
            // background housekeeping must not fail the already-committed write
        } catch (RuntimeException e) {
            // same: the next explicit enforceRetention() cleans up
        }
    }

    /** Deletes on-disk segments that nothing references anymore. Lifecycle lock held. */
    private RetentionResult reclaimUnreferencedFiles(int expiredVersions) {
        Manifest current = manifest;
        Set<String> keep = new LinkedHashSet<>(current.liveSegments());
        for (VersionInfo v : current.history()) {
            keep.addAll(v.segments());
        }
        keep.addAll(snapshots.pinnedSegments());

        // sync point for interruption tests: we are committed, about to delete
        reclaimStepHook.run();
        boolean interrupted = Thread.interrupted();
        if (interrupted) {
            throw new StoreException(ErrorCode.CLEANUP_INTERRUPTED,
                    "retention cleanup interrupted before deleting files; safe to retry");
        }
        int deletedFiles = 0;
        long deletedBytes = 0;
        List<Path> garbage = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.forEach(p -> {
                String name = p.getFileName().toString();
                if (SEGMENT_NAME.matcher(name).matches()) {
                    long seq = Long.parseLong(name.substring(4, 10));
                    segmentSeq.updateAndGet(cur -> Math.max(cur, seq));
                    if (!keep.contains(name)) {
                        garbage.add(p);
                    }
                } else if (name.endsWith(SegmentFormat.TMP_SUFFIX)) {
                    garbage.add(p);
                }
            });
        } catch (IOException e) {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT,
                    "cannot list store dir during cleanup: " + e.getMessage(), e);
        }
        for (Path p : garbage) {
            if (interrupted) {
                throw new StoreException(ErrorCode.CLEANUP_INTERRUPTED,
                        "retention cleanup interrupted after " + deletedFiles
                                + " files (" + deletedBytes + " bytes); safe to retry");
            }
            long size = -1;
            try {
                size = Files.size(p);
            } catch (IOException ignored) {
                // file already gone or unstatable: deletion below is idempotent
            }
            if (deleteIfExists(p)) {
                deletedFiles++;
                if (size > 0) {
                    deletedBytes += size;
                }
            }
            if (Thread.interrupted()) {
                interrupted = true;
            }
        }
        return new RetentionResult(expiredVersions, deletedFiles, deletedBytes);
    }

    private static boolean deleteIfExists(Path p) {
        try {
            return Files.deleteIfExists(p);
        } catch (IOException e) {
            // locked on some platforms or transient failure: leave it for the
            // next pass; it is unreferenced and can never come back logically
            return false;
        }
    }

    // -------------------------------------------------------------- schema

    /** Appends a column; pre-existing rows read back with the type default. */
    public void addColumn(String name, ColumnType type) {
        ensureOpen();
        lifecycleLock.lock();
        try {
            manifestLock.lock();
            try {
                Manifest updated = manifest.withSchema(manifest.schema().addColumn(name, type));
                updated.save(dir);
                manifest = updated;
            } finally {
                manifestLock.unlock();
            }
        } finally {
            lifecycleLock.unlock();
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
        lifecycleLock.lock();
        try {
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
        } finally {
            lifecycleLock.unlock();
        }
    }

    public Manifest currentManifest() {
        return manifest;
    }

    // -------------------------------------------------------------- internals

    private long nextVersionId(Manifest current) {
        VersionInfo head = current.headVersion();
        long id = head == null ? 1 : head.versionId() + 1;
        versionSeq.updateAndGet(cur -> Math.max(cur, id));
        return id;
    }

    private String describeLatest() {
        VersionInfo head = manifest.headVersion();
        return head == null ? "none" : ("v" + head.versionId());
    }

    /** Fills deterministic defaults for columns added after the row was written. */
    private static Map<String, Object> fillDefaults(Map<String, Object> row, Schema schema) {
        Map<String, Object> full = new java.util.LinkedHashMap<>(row);
        for (ColumnDef col : schema.columns()) {
            full.putIfAbsent(col.name(), Schema.defaultValueOf(col.type()));
        }
        return full;
    }

    private void readSegmentsInto(List<String> segmentNames, Schema schema,
                                  List<Map<String, Object>> rows) {
        for (String segmentName : segmentNames) {
            SegmentFormat.SegmentData data;
            try (ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                data = SegmentFormat.read(dir.resolve(segmentName));
            }
            for (Map<String, Object> row : data.rows()) {
                rows.add(fillDefaults(row, schema));
            }
        }
    }

    private void commitManifest(Manifest updated) {
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

    /** Removes tmp files and segments not referenced by the manifest/history. */
    private void cleanupOrphans() {
        Set<String> keep = new LinkedHashSet<>(manifest.liveSegments());
        for (VersionInfo v : manifest.history()) {
            keep.addAll(v.segments());
        }
        try (Stream<Path> files = Files.list(dir)) {
            files.forEach(p -> {
                String name = p.getFileName().toString();
                Matcher m = SEGMENT_NAME.matcher(name);
                if (m.matches()) {
                    segmentSeq.updateAndGet(cur ->
                            Math.max(cur, Long.parseLong(m.group(1))));
                    if (!keep.contains(name)) {
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
        lifecycleLock.lock();
        try {
            closed = true;
            for (VersionSnapshot snapshot : snapshots.activeSnapshots()) {
                snapshot.markClosed();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }
}
