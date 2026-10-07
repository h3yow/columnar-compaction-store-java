package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retention cleanup must expire old data by policy, truly free disk, be safe
 * to interrupt and retry, stay idempotent under repetition, and never disturb
 * live snapshots or concurrent readers.
 */
class RetentionCleanupTest {

    private static final Logger log = LoggerFactory.getLogger(RetentionCleanupTest.class);
    private static final long DAY = 24L * 3600 * 1000;

    @TempDir
    Path dir;

    private final Schema schema = Schema.of(List.of(
            new ColumnDef("id", ColumnType.LONG),
            new ColumnDef("name", ColumnType.STRING)));
    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private ColumnarStore store;

    @AfterEach
    void closeStore() {
        if (store != null) {
            store.close();
        }
    }

    private ColumnarStore openStore() {
        store = ColumnarStore.open(dir, schema, new ResourceLimiter(1 << 22, 16),
                100, SnapshotRegistry.SnapshotLimits.defaults(), clock::get);
        return store;
    }

    private void writeBatch(long from, long to) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            rows.add(Map.of("id", i, "name", "user-" + i));
        }
        log.info("[write] input batch rows={}..{} ({} rows)", from, to, rows.size());
        store.write(new Batch(rows));
    }

    private long segmentFileCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().matches("seg-\\d+\\.ccs")).count();
        }
    }

    @Test
    void maxAgeExpiresOldSegmentsAndFreesDisk() throws IOException {
        openStore();
        writeBatch(1, 10);                       // created at t0
        clock.addAndGet(8 * DAY);                // 8 days pass
        writeBatch(11, 20);                      // fresh segment
        assertEquals(2, segmentFileCount());

        ColumnarStore.RetentionResult r =
                store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7)));
        log.info("[retention] maxAge=7d expired={} reclaimed={} deferred={}",
                r.expiredSegments(), r.reclaimedSegments(), r.deferredToSnapshots());
        assertEquals(1, r.expiredSegments());
        assertEquals(1, r.reclaimedSegments());
        assertEquals(0, r.deferredToSnapshots());
        assertEquals(1, segmentFileCount(), "expired segment file must be deleted");
        assertEquals(10, store.readSnapshot().rows().size());
        assertTrue(store.readSnapshot().rows().stream()
                        .allMatch(row -> ((Long) row.get("id")) >= 11),
                "expired rows must no longer be visible");
        log.info("[retention] PASS: age-based expiry freed disk and hid old rows");
    }

    @Test
    void keepLastVersionsKeepsOnlyNewestK() throws IOException {
        openStore();
        writeBatch(1, 5);
        writeBatch(6, 10);
        writeBatch(11, 15);
        writeBatch(16, 20);

        ColumnarStore.RetentionResult r =
                store.enforceRetention(RetentionPolicy.keepLastVersions(2));
        log.info("[retention] keepLastVersions=2 expired={} reclaimed={}",
                r.expiredSegments(), r.reclaimedSegments());
        assertEquals(2, r.expiredSegments());
        assertEquals(2, segmentFileCount());
        List<Map<String, Object>> rows = store.readSnapshot().rows();
        assertEquals(10, rows.size());
        assertTrue(rows.stream().allMatch(row -> ((Long) row.get("id")) >= 6));
        log.info("[retention] PASS: only the newest 2 versions remain live");
    }

    @Test
    void retentionIsIdempotentAndExpiredDataNeverReappears() throws IOException {
        openStore();
        writeBatch(1, 5);
        writeBatch(6, 10);
        long versionBefore = store.currentManifest().version();

        store.enforceRetention(RetentionPolicy.keepLastVersions(1));
        long versionAfterFirst = store.currentManifest().version();
        assertTrue(versionAfterFirst > versionBefore, "a real cleanup bumps the data version");

        ColumnarStore.RetentionResult second =
                store.enforceRetention(RetentionPolicy.keepLastVersions(1));
        assertEquals(0, second.expiredSegments(), "repeat trigger must be a no-op");
        assertEquals(versionAfterFirst, store.currentManifest().version(),
                "no-op cleanup must not bump the version");
        assertEquals(5, store.readSnapshot().rows().size());

        // reopen: expired rows must not reappear from disk
        store.close();
        openStore();
        List<Map<String, Object>> rows = store.readSnapshot().rows();
        assertEquals(5, rows.size());
        assertTrue(rows.stream().allMatch(row -> ((Long) row.get("id")) >= 6));
        assertEquals(1, segmentFileCount());
        log.info("[retention] PASS: repeat trigger is a no-op; expired data stays gone after reopen");
    }

    @Test
    void interruptedCleanupIsSafeAndRetryCompletesIt() throws IOException {
        openStore();
        writeBatch(1, 5);
        clock.addAndGet(8 * DAY);
        writeBatch(6, 10);

        store.retentionCrashHook = () -> {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT, "simulated crash mid-cleanup");
        };
        StoreException boom = assertThrows(StoreException.class,
                () -> store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7))));
        log.info("[retention] interrupted as injected: code={}", boom.code());

        // the manifest swap already happened: expired rows are invisible, no half state
        assertEquals(5, store.readSnapshot().rows().size());
        assertTrue(store.readSnapshot().rows().stream()
                .allMatch(row -> ((Long) row.get("id")) >= 6));

        store.retentionCrashHook = () -> { };
        ColumnarStore.RetentionResult retry =
                store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7)));
        assertEquals(0, retry.expiredSegments(), "retry finds the manifest already cleaned");
        assertEquals(1, segmentFileCount(),
                "retry must finish reclaiming the interrupted cleanup's files");
        assertEquals(5, store.readSnapshot().rows().size());
        log.info("[retention] PASS: crash mid-cleanup left consistent state; retry reclaimed disk");
    }

    @Test
    void expiredButPinnedSegmentSurvivesUntilSnapshotClose() throws IOException {
        openStore();
        writeBatch(1, 10);
        VersionSnapshot snap = store.openSnapshot();
        clock.addAndGet(8 * DAY);
        writeBatch(11, 20);

        ColumnarStore.RetentionResult r =
                store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7)));
        assertEquals(1, r.expiredSegments());
        assertEquals(1, r.deferredToSnapshots(), "pinned expired segment must be deferred");
        assertEquals(2, segmentFileCount());
        assertEquals(10, store.readSnapshot().rows().size(),
                "live reads no longer see the expired segment");
        assertEquals(10, snap.read().size());
        assertTrue(snap.read().stream().allMatch(row -> ((Long) row.get("id")) <= 10),
                "the open snapshot still sees exactly its own version");
        log.info("[retention] expired segment deferred while pinned; snapshot still consistent");

        snap.close();
        assertEquals(1, segmentFileCount(),
                "releasing the snapshot must reclaim the deferred segment");
        log.info("[retention] PASS: deferred segment reclaimed on snapshot close");
    }

    @Test
    void cleanupCompactionAndReadsInterleavedStayConsistent() throws Exception {
        openStore();
        int batches = 24;
        CountDownLatch start = new CountDownLatch(1);
        List<String> violations = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(4);

        Future<?> writer = pool.submit(() -> {
            await(start);
            for (int b = 0; b < batches; b++) {
                writeBatch(b * 10L + 1, b * 10L + 10);
                clock.addAndGet(DAY / 2);
                sleepQuietly(2);
            }
        });
        Future<?> cleaner = pool.submit(() -> {
            await(start);
            for (int i = 0; i < batches * 2; i++) {
                try {
                    store.compact();
                    store.enforceRetention(RetentionPolicy.keepLastVersions(3));
                } catch (StoreException e) {
                    violations.add("cleanup failed: " + e.code() + " " + e.getMessage());
                }
                sleepQuietly(1);
            }
        });
        List<Future<?>> readers = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
            int id = r;
            readers.add(pool.submit(() -> {
                await(start);
                for (int i = 0; i < batches; i++) {
                    try (VersionSnapshot snap = store.openSnapshot()) {
                        List<Map<String, Object>> first = snap.read();
                        for (int k = 0; k < 3; k++) {
                            if (!first.equals(snap.read())) {
                                violations.add("reader-" + id + ": snapshot read not repeatable");
                            }
                        }
                    } catch (StoreException e) {
                        violations.add("reader-" + id + " failed: " + e.code());
                    }
                    store.readSnapshot(); // must never throw or corrupt
                    sleepQuietly(1);
                }
            }));
        }
        start.countDown();
        writer.get(30, TimeUnit.SECONDS);
        cleaner.get(30, TimeUnit.SECONDS);
        for (Future<?> r : readers) {
            r.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(List.of(), violations, "no consistency violations allowed: " + violations);
        // final state is well-formed and stable: a last cleanup settles everything
        store.compact();
        store.enforceRetention(RetentionPolicy.keepLastVersions(1));
        long files = segmentFileCount();
        int rows = store.readSnapshot().rows().size();
        log.info("[concurrent] violations=0 finalFiles={} finalRows={}"
                + " | basis: cleanup+compaction+snapshot reads interleave safely", files, rows);
        assertTrue(files <= 2, "settled store keeps at most the live segment(s), got " + files);
        assertTrue(rows > 0);
    }

    @Test
    void legacyManifestWithoutTimestampsIsCompatible() throws IOException {
        // hand-build a pre-feature store: old-format manifest (bare segment
        // names, no version field) plus a valid segment file
        Files.createDirectories(dir);
        SegmentFormat.write(dir.resolve("seg-000001.ccs"), schema,
                List.of(Map.of("id", 1L, "name", "legacy-1"),
                        Map.of("id", 2L, "name", "legacy-2")));
        String legacyManifest = """
                {
                  "schemaVersion": 1,
                  "columns": [
                    {"name": "id", "type": "LONG"},
                    {"name": "name", "type": "STRING"}
                  ],
                  "segments": ["seg-000001.ccs"],
                  "compactionGeneration": 0
                }
                """;
        Files.writeString(dir.resolve(Manifest.FILE_NAME), legacyManifest);

        openStore();
        assertEquals(2, store.readSnapshot().rows().size(),
                "legacy segment must be readable");
        assertEquals(0, store.currentManifest().version(),
                "legacy manifest starts at data version 0");

        // freshly stamped legacy segments are not expired right after upgrade
        ColumnarStore.RetentionResult r =
                store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7)));
        assertEquals(0, r.expiredSegments());
        // but they do expire once they actually age out
        clock.addAndGet(8 * DAY);
        r = store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7)));
        assertEquals(1, r.expiredSegments());
        assertEquals(0, store.readSnapshot().rows().size());
        log.info("[compat] PASS: legacy manifest loads, is not expired on upgrade,"
                + " and ages out normally afterwards");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
