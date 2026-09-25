package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction must be idempotent, crash-safe and invisible to concurrent
 * readers except as an atomic snapshot swap.
 */
class CompactionTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionTest.class);

    @TempDir
    Path dir;

    private ColumnarStore store;
    private final Schema schema = Schema.of(List.of(
            new ColumnDef("id", ColumnType.LONG),
            new ColumnDef("name", ColumnType.STRING)));

    @BeforeEach
    void openStore() {
        store = ColumnarStore.open(dir, schema, new ResourceLimiter(1 << 22, 16));
    }

    private void writeBatch(long from, long to) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            rows.add(Map.of("id", i, "name", "user-" + i));
        }
        Batch batch = new Batch(rows);
        log.info("[write] input batch rows={}..{} ({} rows)", from, to, rows.size());
        store.write(batch);
    }

    @Test
    void repeatedCompactionIsIdempotent() {
        writeBatch(1, 10);
        writeBatch(11, 20);
        writeBatch(21, 30);
        log.info("[idempotent] live segments before={}", store.currentManifest().liveSegments());

        assertTrue(store.compact(), "first compaction should merge");
        long genAfterFirst = store.currentManifest().compactionGeneration();
        List<Map<String, Object>> afterFirst = store.readSnapshot().rows();

        assertFalse(store.compact(), "second compaction must be a no-op");
        assertFalse(store.compact(), "third compaction must be a no-op");
        List<Map<String, Object>> afterRepeats = store.readSnapshot().rows();

        log.info("[idempotent] generation after first={} after repeats={} | rows after first={} after repeats={}",
                genAfterFirst, store.currentManifest().compactionGeneration(),
                afterFirst.size(), afterRepeats.size());
        log.info("[idempotent] decision basis: single live segment => no merge, no double count");
        assertEquals(genAfterFirst, store.currentManifest().compactionGeneration(),
                "generation must not advance on no-op compactions");
        assertEquals(30, afterRepeats.size());
        assertEquals(afterFirst, afterRepeats, "rows must be identical across repeated compactions");
        assertEquals(1, store.currentManifest().liveSegments().size());
    }

    @Test
    void dataIsLogicallyIdenticalAcrossCompaction() {
        writeBatch(1, 5);
        writeBatch(6, 10);
        List<Map<String, Object>> before = store.readSnapshot().rows();
        store.compact();
        List<Map<String, Object>> after = store.readSnapshot().rows();
        log.info("[consistency] rows before={} after={} | basis: compaction preserves logical data",
                before, after);
        assertEquals(before, after);
    }

    @Test
    void crashedCompactionLeavesSnapshotIntactAndRecoverable() {
        writeBatch(1, 10);
        writeBatch(11, 20);
        List<Map<String, Object>> expected = store.readSnapshot().rows();
        long genBefore = store.currentManifest().compactionGeneration();

        // simulate a crash after the merged segment is durable but before the swap
        store.compactionCrashHook = () -> {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT, "simulated crash mid-compaction");
        };
        try {
            store.compact();
        } catch (StoreException e) {
            log.info("[crash] compaction failed as injected: code={} msg='{}'", e.code(), e.getMessage());
        }
        List<Map<String, Object>> afterCrash = store.readSnapshot().rows();
        log.info("[crash] rows after failed compaction={} | basis: old manifest untouched => no half state",
                afterCrash.size());
        assertEquals(expected, afterCrash, "failed compaction must not alter visible data");
        assertEquals(genBefore, store.currentManifest().compactionGeneration());

        // recovery: clear the hook, compaction succeeds and still no duplicates
        store.compactionCrashHook = () -> { };
        assertTrue(store.compact());
        List<Map<String, Object>> afterRecovery = store.readSnapshot().rows();
        log.info("[crash] rows after recovery={} | basis: retry merges the same inputs exactly once",
                afterRecovery.size());
        assertEquals(expected, afterRecovery);
        assertEquals(1, store.currentManifest().liveSegments().size());
    }

    @Test
    void concurrentReadersAlwaysSeeACompleteSnapshot() throws Exception {
        writeBatch(1, 50);
        writeBatch(51, 100);
        writeBatch(101, 150);
        List<Map<String, Object>> fullData = store.readSnapshot().rows();
        int expectedRows = fullData.size();

        // gate the crash hook so readers run while compaction is mid-flight
        CountDownLatch compactionMidFlight = new CountDownLatch(1);
        CountDownLatch releaseCompaction = new CountDownLatch(1);
        store.compactionCrashHook = () -> {
            compactionMidFlight.countDown();
            try {
                releaseCompaction.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<String> violations = new CopyOnWriteArrayList<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        try {
            Future<?> compactor = pool.submit(() -> store.compact());
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 3; r++) {
                int readerId = r;
                readers.add(pool.submit(() -> {
                    while (!stop.get()) {
                        List<Map<String, Object>> rows = store.readSnapshot().rows();
                        log.info("[reader-{}] observed {} rows", readerId, rows.size());
                        if (rows.size() != expectedRows) {
                            violations.add("reader-" + readerId + " saw " + rows.size()
                                    + " rows, expected " + expectedRows);
                        }
                        // every observed row must be a full row from the written data
                        for (Map<String, Object> row : rows) {
                            long id = (Long) row.get("id");
                            if (!("user-" + id).equals(row.get("name"))) {
                                violations.add("reader-" + readerId + " saw torn row " + row);
                            }
                        }
                    }
                }));
            }
            assertTrue(compactionMidFlight.await(10, TimeUnit.SECONDS),
                    "compaction should reach the mid-flight gate");
            // readers hammer the store while the merged segment exists but is invisible
            Thread.sleep(100);
            releaseCompaction.countDown();
            compactor.get(10, TimeUnit.SECONDS);
            stop.set(true);
            for (Future<?> reader : readers) {
                reader.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        log.info("[concurrent] violations={} | basis: snapshot reads are atomic w.r.t. manifest swap",
                violations);
        assertTrue(violations.isEmpty(), () -> "snapshot violations: " + violations);
        assertEquals(expectedRows, store.readSnapshot().rows().size());
    }

    @Test
    void concurrentDuplicateCompactionTriggersDoNotDuplicate() throws Exception {
        writeBatch(1, 20);
        writeBatch(21, 40);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<Boolean>> triggers = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                triggers.add(pool.submit(() -> store.compact()));
            }
            int mergedCount = 0;
            for (Future<Boolean> t : triggers) {
                if (t.get(10, TimeUnit.SECONDS)) {
                    mergedCount++;
                }
            }
            log.info("[dup-trigger] triggers that actually merged={} | basis: compaction is serialized"
                    + " and no-op on a single-segment live set", mergedCount);
            assertEquals(1, mergedCount, "exactly one trigger may perform the merge");
            assertEquals(40, store.readSnapshot().rows().size(), "no duplicated rows");
            assertEquals(1, store.currentManifest().compactionGeneration(),
                    "generation advances exactly once");
        } finally {
            pool.shutdownNow();
        }
    }
}
