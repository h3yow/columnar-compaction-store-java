package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Version snapshots: consistency across writes/compaction/cleanup, retention
 * expiration with real disk reclamation, snapshot count/byte caps and
 * release, concurrent interleaving, and failure/retry behavior.
 */
class VersionSnapshotTest {

    private static final Logger log = LoggerFactory.getLogger(VersionSnapshotTest.class);

    private static final Schema SCHEMA = Schema.of(List.of(
            new ColumnDef("id", ColumnType.LONG),
            new ColumnDef("name", ColumnType.STRING)));

    /** Controllable clock so age-based retention is deterministic. */
    static final class MutableClock extends Clock {
        private final AtomicLong millis;
        private final ZoneId zone = ZoneOffset.UTC;

        MutableClock(long start) {
            this.millis = new AtomicLong(start);
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }

        long now() {
            return millis.get();
        }

        void advance(long deltaMillis) {
            millis.addAndGet(deltaMillis);
        }
    }

    private void writeRows(ColumnarStore store, long from, long to) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            rows.add(Map.of("id", i, "name", "user-" + i));
        }
        store.write(new Batch(rows));
    }

    private List<Map<String, Object>> expectedRows(long from, long to) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            rows.add(Map.of("id", i, "name", "user-" + i));
        }
        return rows;
    }

    private long countSegmentFiles(Path dir) throws Exception {
        try (var files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".ccs")).count();
        }
    }

    private ColumnarStore openWith(Path dir, MutableClock clock,
                                   RetentionPolicy retention, SnapshotLimits limits) {
        return ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(1 << 24, 16),
                50, retention, limits, clock);
    }

    @Test
    void failedRetentionCommitLeavesDataIntactAndRetrySucceeds(@TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA,
                new ResourceLimiter(1 << 24, 16), 5_000,
                RetentionPolicy.keepVersions(100), new SnapshotLimits(8, 1 << 30), clock);
        writeRows(store, 1, 2);                          // v1
        clock.advance(1000);
        writeRows(store, 3, 4);                          // v2
        assertEquals(2, store.versions().size());
        store.replaceRetentionPolicy(RetentionPolicy.currentOnly());

        // crash hook fires at the start of an explicit retention pass: no commit happens
        store.retentionCrashHook = () -> {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT, "simulated crash before trim");
        };
        StoreException e = assertThrows(StoreException.class, store::enforceRetention);
        log.info("[retry] retention crashed pre-commit: code={}", e.code());
        assertEquals(2, store.versions().size(), "failed cleanup must not trim anything");
        assertEquals(expectedRows(1, 2), store.openSnapshot(1L).readRows(),
                "data is intact after a failed cleanup");

        store.retentionCrashHook = () -> { };
        ColumnarStore.RetentionResult result = store.enforceRetention();
        log.info("[retry] retry result={}; versions now={}", result, store.versions().size());
        assertEquals(1, store.versions().size(), "retry completes the trim exactly once");
        assertEquals(expectedRows(1, 4), store.readSnapshot().rows());
        StoreException gone = assertThrows(StoreException.class,
                () -> store.openSnapshot(1L));
        assertEquals(ErrorCode.SNAPSHOT_VERSION_EXPIRED, gone.code());
    }

    @Test
    void snapshotReadAfterExternalDataLossFailsWithDistinctCodeNotEmptyResult(
            @TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(100),
                new SnapshotLimits(8, 1 << 30));
        writeRows(store, 1, 3);
        VersionSnapshot snapshot = store.openSnapshot();
        String backing = snapshot.segments().get(0);

        // simulate external damage the store did not cause (disk accident / manual rm)
        Files.delete(dir.resolve(backing));
        StoreException e = assertThrows(StoreException.class, snapshot::read);
        log.info("[unavailable] read with missing backing file rejected code={} msg='{}'",
                e.code(), e.getMessage());
        assertEquals(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE, e.code(),
                "must fail loudly with a distinct reason, never return empty/partial rows");
    }

    @Test
    void versionsPersistAcrossReopenAndOldManifestIsUpgraded(@TempDir Path dir)
            throws Exception {
        // first life: create data and some versions
        ColumnarStore first = ColumnarStore.open(dir, SCHEMA,
                new ResourceLimiter(1 << 24, 16), 50,
                RetentionPolicy.keepVersions(10), new SnapshotLimits(8, 1 << 30),
                new MutableClock(5_000_000));
        writeRows(first, 1, 2);
        writeRows(first, 3, 4);
        VersionInfo v1Before = first.versions().get(0);
        first.close();

        // second life: catalog must survive reopen; old v1 still openable
        ColumnarStore second = ColumnarStore.open(dir, SCHEMA,
                new ResourceLimiter(1 << 24, 16), 50,
                RetentionPolicy.keepVersions(10), new SnapshotLimits(8, 1 << 30),
                new MutableClock(5_000_000));
        log.info("[reopen] versions after reopen={}", second.versions());
        assertEquals(2, second.versions().size());
        try (VersionSnapshot old = second.openSnapshot(v1Before.versionId())) {
            assertEquals(expectedRows(1, 2), old.readRows(),
                    "historical snapshot reads across process restart");
        }
        assertEquals(expectedRows(1, 4), second.readSnapshot().rows());
    }

    @Test
    void snapshotOfVersionWithSchemaEvolutionReturnsOriginalColumns(
            @TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                new SnapshotLimits(8, 1 << 30));
        writeRows(store, 1, 2);
        VersionSnapshot old = store.openSnapshot();
        store.addColumn("score", ColumnType.INT);
        log.info("[schema] added column after snapshot; head columns={}",
                store.currentManifest().schema().columns());

        ColumnarStore.Snapshot view = old.read();
        assertEquals(2, view.schema().columnCount(),
                "snapshot keeps the schema as of its version");
        assertEquals(List.of("id", "name"),
                view.schema().columns().stream().map(ColumnDef::name).toList());
        for (Map<String, Object> row : view.rows()) {
            assertFalse(row.containsKey("score"), "new column must not leak into the old snapshot");
        }
        // head rows materialize the default for the added column
        for (Map<String, Object> row : store.readSnapshot().rows()) {
            assertEquals(0, row.get("score"));
        }
        log.info("[schema] old snapshot rows={} (no score column leaked)", view.rows());
        old.close();
    }

    @Test
    void interruptedCleanupIsReportedDistinctlyAndRetryFinishesIt(@TempDir Path dir)
            throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA,
                new ResourceLimiter(1 << 24, 16), 5_000,
                RetentionPolicy.keepVersions(100), new SnapshotLimits(8, 1 << 30), clock);

        // Several "generations": each generation = two small writes + compaction.
        // Compaction creates a merged segment; older merged segments stay on disk
        // because the loose policy keeps all versions.
        long id = 1;
        for (int gen = 0; gen < 8; gen++) {
            writeRows(store, id, id + 4);
            id += 5;
            clock.advance(1);
            writeRows(store, id, id + 4);
            id += 5;
            clock.advance(1);
            assertTrue(store.compact(), "generation " + gen + " compacts two segments");
        }
        long filesBefore = countSegmentFiles(dir);
        assertTrue(filesBefore >= 8, "multiple compacted generations left on disk, was "
                + filesBefore);

        // switch to current-only: the explicit pass expires old versions and makes
        // old merged segments garbage, but we interrupt it inside the reclaim phase
        store.replaceRetentionPolicy(RetentionPolicy.currentOnly());
        java.util.concurrent.CountDownLatch inReclaim =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<StoreException> workerError =
                new java.util.concurrent.atomic.AtomicReference<>();
        store.reclaimStepHook = new Runnable() {
            private boolean armed = true;

            @Override
            public void run() {
                if (armed) {
                    armed = false;
                    inReclaim.countDown();
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };

        Thread worker = new Thread(() -> {
            try {
                store.enforceRetention();
            } catch (StoreException e) {
                workerError.set(e);
            }
        });
        worker.start();
        assertTrue(inReclaim.await(5, java.util.concurrent.TimeUnit.SECONDS));
        worker.interrupt();
        worker.join(5_000);
        assertFalse(worker.isAlive(), "interrupted cleanup must return promptly");

        StoreException interrupted = workerError.get();
        log.info("[interrupt] interrupted cleanup code={} msg='{}'",
                interrupted == null ? null : interrupted.code(),
                interrupted == null ? null : interrupted.getMessage());
        assertEquals(ErrorCode.CLEANUP_INTERRUPTED, interrupted.code(),
                "interruption must surface a distinct, retryable reason");

        store.reclaimStepHook = () -> { };
        ColumnarStore.RetentionResult retry = store.enforceRetention();
        ColumnarStore.RetentionResult followUp = store.enforceRetention();
        long filesAfter = countSegmentFiles(dir);
        log.info("[interrupt] files {} -> retry deletedFiles={} followUp={} filesRemaining={}",
                filesBefore, retry.deletedFiles(), followUp, filesAfter);
        assertTrue(filesAfter < filesBefore, "retry really frees disk space");
        assertEquals(1, filesAfter, "only the head live segment remains");
        assertEquals(0, followUp.deletedFiles(), "completed cleanup is idempotent");
        assertEquals(expectedRows(1, id - 1), store.readSnapshot().rows(),
                "interrupting cleanup never damages the live table");
    }

    @Test
    void repeatedMixOfCompactionRetentionAndReleaseIsStableAndIdempotent(
            @TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(3),
                new SnapshotLimits(4, 1 << 30));

        for (int round = 1; round <= 6; round++) {
            writeRows(store, round * 10L - 9, round * 10L);
            clock.advance(1000);
            store.compact();
            try (VersionSnapshot snap = store.openSnapshot()) {
                assertEquals(10 * round, snap.readRows().size());
                store.enforceRetention();
                store.enforceRetention(); // duplicate trigger: must be a no-op
                store.compact();         // duplicate trigger on one live segment
            }
            assertEquals(0, store.activeSnapshotCount(),
                    "round " + round + ": no leaked snapshot count");
            assertEquals(0, store.pinnedBytes(),
                    "round " + round + ": no leaked pinned bytes");
        }
        log.info("[mix] after 6 mixed rounds: versions={} activeSnapshots={} pinnedBytes={} rows={}",
                store.versions().size(), store.activeSnapshotCount(),
                store.pinnedBytes(), store.readSnapshot().rows().size());
        assertTrue(store.versions().size() <= 3, "retention keeps at most 3 versions");
        assertEquals(expectedRows(1, 60), store.readSnapshot().rows());
        // catalog head still openable; oldest versions expired
        StoreException e = assertThrows(StoreException.class, () -> store.openSnapshot(1L));
        assertEquals(ErrorCode.SNAPSHOT_VERSION_EXPIRED, e.code());
    }

    @Test
    void snapshotStaysConsistentAcrossWritesCompactionAndCleanup(@TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                new SnapshotLimits(8, 1 << 30));

        writeRows(store, 1, 5);
        VersionSnapshot v1 = store.openSnapshot();
        long v1Id = v1.versionId();
        log.info("[snap-stable] opened snapshot of v{} with 5 rows", v1Id);

        clock.advance(1000);
        writeRows(store, 6, 10);
        assertTrue(store.compact(), "second batch => compaction merges");
        clock.advance(1000);
        writeRows(store, 11, 15);
        store.enforceRetention();
        log.info("[snap-stable] after more writes+compaction+cleanup head has {} rows",
                store.readSnapshot().rows().size());

        List<Map<String, Object>> rows = v1.readRows();
        log.info("[snap-stable] pinned snapshot v1 still returns {} rows: {}", rows.size(), rows);
        assertEquals(expectedRows(1, 5), rows,
                "snapshot must not be polluted by later writes/compactions");
        assertEquals(15, store.readSnapshot().rows().size());

        // re-reading repeatedly stays identical
        assertEquals(rows, v1.readRows());
        v1.close();
    }

    @Test
    void expiredVersionIsRejectedAndItsFilesAreReallyDeleted(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(2),
                new SnapshotLimits(8, 1 << 30));

        writeRows(store, 1, 3);                          // v1, seg1
        long v1Id = store.versions().get(0).versionId();
        List<String> v1Segments = List.copyOf(store.versions().get(0).segments());
        clock.advance(1000);
        writeRows(store, 4, 6);                          // v2, live=seg1+seg2
        assertTrue(store.compact());                     // v3, live=seg3
        clock.advance(1000);
        writeRows(store, 7, 9);                          // v4, live=seg3+seg4

        // committing v4 with keepVersions(2) expires v1+v2; seg1/seg2 are then
        // referenced by no retained version and are physically reclaimed
        log.info("[snap-expire] post-write .ccs files on disk={} (seg1/seg2 should be gone)",
                countSegmentFiles(dir));
        for (String name : v1Segments) {
            assertFalse(Files.exists(dir.resolve(name)),
                    "segment " + name + " exclusively backing expired versions must be deleted");
        }

        StoreException e = assertThrows(StoreException.class, () -> store.openSnapshot(v1Id));
        log.info("[snap-expire] reopen expired v{} rejected code={} msg='{}'",
                v1Id, e.code(), e.getMessage());
        assertEquals(ErrorCode.SNAPSHOT_VERSION_EXPIRED, e.code());

        // repeated explicit cleanup: stable, idempotent, no resurrection
        ColumnarStore.RetentionResult again = store.enforceRetention();
        assertEquals(0, again.deletedFiles());
        for (String name : v1Segments) {
            assertFalse(Files.exists(dir.resolve(name)));
        }
        log.info("[snap-expire] repeated cleanup result={} | basis: idempotent, no resurrection",
                again);
        assertEquals(expectedRows(1, 9), store.readSnapshot().rows(),
                "expiring old versions never touches the live table");
    }

    @Test
    void ageBasedRetentionDeletesDataOlderThanTheWindow(@TempDir Path dir) throws Exception {
        long day = 24L * 60 * 60 * 1000;
        MutableClock clock = new MutableClock(10 * day);
        // keep everything while writing; apply the age window explicitly later
        ColumnarStore store = openWith(dir, clock,
                RetentionPolicy.keepVersions(100), new SnapshotLimits(8, 1 << 30));

        writeRows(store, 1, 2);                          // t=10d, v1, seg1
        clock.advance(1);
        writeRows(store, 3, 4);                          // t=10d, v2, seg1+seg2
        String oldSegment = store.versions().get(0).segments().get(0);
        assertTrue(store.compact());                     // v3, seg3 merged
        clock.advance(3 * day);                          // t=13d
        writeRows(store, 5, 6);                          // v4
        assertEquals(4, countSegmentFiles(dir), "no expiry yet under the loose policy");

        store.replaceRetentionPolicy(RetentionPolicy.ofAge(2 * day, 100));
        ColumnarStore.RetentionResult result = store.enforceRetention();
        log.info("[snap-age] window=2d expired={} deletedFiles={} bytes={}, files now={}",
                result.expiredVersions(), result.deletedFiles(), result.deletedBytes(),
                countSegmentFiles(dir));

        assertEquals(3, result.expiredVersions(),
                "v1..v3 are older than 2 days; head v4 always stays");
        assertTrue(result.deletedFiles() >= 2, "old data files must be physically deleted");
        assertTrue(result.deletedBytes() > 0, "reclaimed space must be reported");
        assertFalse(Files.exists(dir.resolve(oldSegment)),
                "the original write segment must be physically removed, returning disk space");
        assertEquals(expectedRows(1, 6), store.readSnapshot().rows());

        StoreException e = assertThrows(StoreException.class,
                () -> store.openSnapshot(1L));
        assertEquals(ErrorCode.SNAPSHOT_VERSION_EXPIRED, e.code());
        log.info("[snap-age] opening expired v1 rejected code={}", e.code());
    }

    @Test
    void snapshotCountLimitRejectsWithDistinctCodeThenAcceptsAfterRelease(@TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                new SnapshotLimits(2, 1 << 30));
        writeRows(store, 1, 2);
        try (VersionSnapshot s1 = store.openSnapshot();
             VersionSnapshot s2 = store.openSnapshot()) {
            log.info("[cap-count] two snapshots open, active={}", store.activeSnapshotCount());
            StoreException e = assertThrows(StoreException.class, store::openSnapshot);
            log.info("[cap-count] third open rejected code={} msg='{}'", e.code(), e.getMessage());
            assertEquals(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED, e.code());
            assertEquals(2, store.activeSnapshotCount(), "rejected open must not count");
        }
        assertEquals(0, store.activeSnapshotCount(), "release hands capacity back");
        VersionSnapshot again = store.openSnapshot();
        log.info("[cap-count] open after close succeeds, active={}", store.activeSnapshotCount());
        assertEquals(1, store.activeSnapshotCount());
        again.close();
        again.close(); // idempotent double release
        assertEquals(0, store.activeSnapshotCount(), "double close does not double-release");
    }

    @Test
    void pinnedBytesLimitRejectsAndSharedSegmentsCountOnce(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore seeder = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                new SnapshotLimits(10, 1 << 30));
        writeRows(seeder, 1, 50);
        long firstSize = java.util.Arrays.stream(dir.toFile().listFiles((d, n) -> n.endsWith(".ccs")))
                .mapToLong(f -> f.length()).sum();
        seeder.close();
        // reopen with a budget that holds one copy of v1, but not the v1+v2 union
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                new SnapshotLimits(10, firstSize + firstSize / 2));
        VersionSnapshot first = store.openSnapshot();
        long firstBytes = store.pinnedBytes();
        log.info("[cap-bytes] first snapshot pinned={} bytes, budget={}",
                firstBytes, firstSize + firstSize / 2);
        assertTrue(firstBytes > 0);

        // opening the SAME version shares all segments: union stays identical
        VersionSnapshot same = store.openSnapshot(first.versionId());
        assertEquals(firstBytes, store.pinnedBytes(),
                "snapshots of the same version share segments: union counted once");
        assertEquals(2, store.activeSnapshotCount());
        same.close();

        // a newer version adds new segments; under the tiny budget it must be rejected
        clock.advance(1000);
        writeRows(store, 51, 120);
        StoreException e = assertThrows(StoreException.class, store::openSnapshot);
        log.info("[cap-bytes] bigger version rejected code={} msg='{}'", e.code(), e.getMessage());
        assertEquals(ErrorCode.SNAPSHOT_BYTES_LIMIT_EXCEEDED, e.code());
        assertEquals(firstBytes, store.pinnedBytes(), "rejected open pins nothing");
        assertEquals(1, store.activeSnapshotCount());

        first.close();
        assertEquals(0, store.pinnedBytes(), "closing the last holder returns all bytes");
        log.info("[cap-bytes] after release pinnedBytes={}", store.pinnedBytes());
    }

    @Test
    void longRunningSnapshotStaysCorrectUnderConcurrentWritesCompactionAndCleanup(
            @TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(30),
                new SnapshotLimits(32, 1 << 30));
        writeRows(store, 1, 10);
        List<Map<String, Object>> expectedV1 = expectedRows(1, 10);
        VersionSnapshot longRunning = store.openSnapshot(); // analytical job starts

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(3);
        java.util.List<String> violations = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicBoolean stop =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(2);
        try {
            // readers keep hammering BOTH the pinned version snapshot and head
            pool.submit(() -> {
                started.countDown();
                long reads = 0;
                while (!stop.get()) {
                    if (!expectedV1.equals(longRunning.readRows())) {
                        violations.add("pinned snapshot changed at read #" + reads);
                    }
                    reads++;
                }
                log.info("[interleave] long-snapshot performed {} reads, all identical", reads);
            });
            // background mutator: write + compact + cleanup over and over
            java.util.concurrent.Future<?> mutator = pool.submit(() -> {
                started.countDown();
                long from = 11;
                for (int round = 0; round < 12 && !stop.get(); round++) {
                    writeRows(store, from, from + 9);
                    from += 10;
                    clock.advance(1000);
                    if (store.currentManifest().liveSegments().size() > 1) {
                        store.compact();
                    }
                    store.enforceRetention();
                }
            });
            started.await();
            mutator.get(60, java.util.concurrent.TimeUnit.SECONDS);
            stop.set(true);
            pool.shutdown();
            pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        log.info("[interleave] violations={} headRows={} versionsRetained={}",
                violations, store.readSnapshot().rows().size(), store.versions().size());
        assertTrue(violations.isEmpty(), () -> "snapshot violations: " + violations);
        assertEquals(expectedV1, longRunning.readRows(),
                "analytical snapshot remains byte-identical to its open moment");
        assertEquals(130, store.readSnapshot().rows().size(), "head advanced to 130 rows");
        longRunning.close();
        assertEquals(0, store.activeSnapshotCount());
        assertEquals(0, store.pinnedBytes(), "all pinned capacity returned after the job ends");

        // one more cleanup after release fully reclaims and stays idempotent
        ColumnarStore.RetentionResult r1 = store.enforceRetention();
        ColumnarStore.RetentionResult r2 = store.enforceRetention();
        log.info("[interleave] cleanup after release: {} then {} | no double counting", r1, r2);
        assertEquals(0, r2.deletedFiles());
        assertEquals(expectedRows(1, 130), store.readSnapshot().rows());
    }

    @Test
    void snapshotCapQueuesAndAcceptsAfterConcurrentRelease(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA,
                new ResourceLimiter(1 << 24, 16), 5_000,
                RetentionPolicy.keepVersions(10), new SnapshotLimits(1, 1 << 30), clock);
        writeRows(store, 1, 3);
        VersionSnapshot holder = store.openSnapshot();

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<VersionSnapshot> waiter =
                    pool.submit(() -> store.openSnapshot());
            Thread.sleep(150); // waiter is queued on capacity
            log.info("[queue] waiter queued; active={}", store.activeSnapshotCount());
            holder.close(); // releases capacity; the queued open must succeed
            VersionSnapshot acquired = waiter.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1, store.activeSnapshotCount());
            assertEquals(expectedRows(1, 3), acquired.readRows());
            log.info("[queue] queued open acquired after release; active={}",
                    store.activeSnapshotCount());
            acquired.close();
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, store.activeSnapshotCount());
        assertEquals(0, store.pinnedBytes());
    }

    @Test
    void closedSnapshotReadsAreRejected(@TempDir Path dir) {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(10),
                SnapshotLimits.defaults());
        writeRows(store, 1, 2);
        VersionSnapshot snapshot = store.openSnapshot();
        assertEquals(2, snapshot.readRows().size());
        snapshot.close();
        StoreException e = assertThrows(StoreException.class, snapshot::read);
        log.info("[closed] read after close rejected code={}", e.code());
        assertEquals(ErrorCode.SNAPSHOT_CLOSED, e.code());
    }

    @Test
    void pinnedSnapshotProtectsItsFilesFromCleanup(@TempDir Path dir) throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        ColumnarStore store = openWith(dir, clock, RetentionPolicy.keepVersions(100),
                new SnapshotLimits(8, 1 << 30));

        writeRows(store, 1, 3);
        VersionSnapshot old = store.openSnapshot();       // pin v1
        String pinnedSegment = old.segments().get(0);
        clock.advance(1000);
        writeRows(store, 4, 6);
        assertTrue(store.compact());

        // swap in an aggressive current-only policy: v1 leaves the window, but
        // its file must survive because the open snapshot pins it
        store.replaceRetentionPolicy(RetentionPolicy.currentOnly());
        ColumnarStore.RetentionResult result = store.enforceRetention();
        log.info("[snap-pin] aggressive cleanup deletedFiles={} pinned segment exists={}",
                result.deletedFiles(), Files.exists(dir.resolve(pinnedSegment)));
        assertTrue(Files.exists(dir.resolve(pinnedSegment)),
                "a valid open snapshot must keep its backing file");
        assertEquals(expectedRows(1, 3), old.readRows(),
                "snapshot still reads exactly its opened version");
        assertTrue(store.versions().size() == 1, "catalog trimmed to head");

        old.close();
        ColumnarStore.RetentionResult afterRelease = store.enforceRetention();
        log.info("[snap-pin] after release cleanup deletedFiles={} segment exists={}",
                afterRelease.deletedFiles(), Files.exists(dir.resolve(pinnedSegment)));
        assertFalse(Files.exists(dir.resolve(pinnedSegment)),
                "releasing the snapshot must allow real space reclamation");
    }
}
