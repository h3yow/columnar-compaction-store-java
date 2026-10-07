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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An explicitly opened {@link VersionSnapshot} must keep serving exactly the
 * logical data of its open instant — across later writes, compactions and
 * retention cleanups — and must release all held resources on close.
 */
class VersionSnapshotTest {

    private static final Logger log = LoggerFactory.getLogger(VersionSnapshotTest.class);
    private static final long WAIT_MILLIS = 200;

    @TempDir
    Path dir;

    private final Schema schema = Schema.of(List.of(
            new ColumnDef("id", ColumnType.LONG),
            new ColumnDef("name", ColumnType.STRING)));
    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private ColumnarStore store;

    @AfterEach
    void closeStore() {
        if (store != null) {
            store.close();
        }
    }

    private ColumnarStore openStore(SnapshotRegistry.SnapshotLimits limits) {
        store = ColumnarStore.open(dir, schema, new ResourceLimiter(1 << 22, 16),
                WAIT_MILLIS, limits, clock::get);
        return store;
    }

    private ColumnarStore openStore() {
        return openStore(SnapshotRegistry.SnapshotLimits.defaults());
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
    void snapshotIsStableAcrossWritesCompactionAndCleanup() {
        openStore();
        writeBatch(1, 10);
        VersionSnapshot snap = store.openSnapshot();
        List<Map<String, Object>> atOpen = snap.read();
        log.info("[snapshot] opened at version={} rows={}", snap.version(), atOpen.size());

        writeBatch(11, 20);
        writeBatch(21, 30);
        store.compact();
        store.enforceRetention(RetentionPolicy.keepLastVersions(1));
        log.info("[snapshot] after 2 writes + compaction + retention, live rows={}",
                store.readSnapshot().rows().size());

        List<Map<String, Object>> after = snap.read();
        assertEquals(atOpen, after,
                "snapshot content must not be polluted by later writes/compaction/cleanup");
        assertEquals(10, after.size());
        assertEquals(atOpen, snap.read(), "snapshot reads must be repeatable");
        snap.close();
        log.info("[snapshot] PASS: pinned snapshot survived writes+compaction+cleanup unchanged"
                + " | basis: manifest pin + segment pin");
    }

    @Test
    void readOnClosedSnapshotFailsWithSnapshotClosed() {
        openStore();
        writeBatch(1, 5);
        VersionSnapshot snap = store.openSnapshot();
        snap.close();
        snap.close(); // idempotent
        StoreException e = assertThrows(StoreException.class, snap::read);
        assertEquals(ErrorCode.SNAPSHOT_CLOSED, e.code());
        log.info("[snapshot] closed read rejected: code={} | basis: distinguishable failure", e.code());
    }

    @Test
    void readAfterStoreCloseFailsWithStoreClosed() {
        openStore();
        writeBatch(1, 5);
        VersionSnapshot snap = store.openSnapshot();
        store.close();
        StoreException e = assertThrows(StoreException.class, snap::read);
        assertEquals(ErrorCode.STORE_CLOSED, e.code());
        log.info("[snapshot] read after store close: code={} | basis: snapshot cannot outlive store",
                e.code());
    }

    @Test
    void snapshotCountLimitIsDistinguishableAndRecoversAfterRelease() {
        openStore(new SnapshotRegistry.SnapshotLimits(1, 1L << 30));
        writeBatch(1, 5);
        VersionSnapshot first = store.openSnapshot();
        StoreException e = assertThrows(StoreException.class, store::openSnapshot);
        assertEquals(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED, e.code());
        log.info("[limit] second snapshot rejected: code={} msg='{}'", e.code(), e.getMessage());

        first.close();
        assertEquals(0, store.openSnapshotCount());
        assertEquals(0, store.snapshotPinnedBytes(),
                "released snapshot must return its pinned-bytes budget");
        VersionSnapshot second = store.openSnapshot(); // slot available again
        assertEquals(5, second.read().size());
        second.close();
        log.info("[limit] PASS: slot returned after close, new snapshot opened"
                + " | basis: release truly frees capacity");
    }

    @Test
    void snapshotOpenQueuesUntilASlotIsReleased() throws Exception {
        openStore(new SnapshotRegistry.SnapshotLimits(1, 1L << 30));
        writeBatch(1, 5);
        VersionSnapshot first = store.openSnapshot();

        CountDownLatch closed = new CountDownLatch(1);
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            first.close();
            closed.countDown();
        });
        releaser.start();

        // default wait in this store is WAIT_MILLIS; open a store view with a
        // longer wait by retrying in a loop until the slot frees up
        AtomicReference<VersionSnapshot> queued = new AtomicReference<>();
        Thread opener = new Thread(() -> {
            while (queued.get() == null) {
                try {
                    queued.set(store.openSnapshot());
                } catch (StoreException e) {
                    assertEquals(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED, e.code());
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        opener.start();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        opener.join(5_000);
        assertTrue(queued.get() != null, "queued snapshot open must succeed after release");
        assertEquals(5, queued.get().read().size());
        queued.get().close();
        log.info("[limit] PASS: queued open succeeded once the slot was released");
    }

    @Test
    void pinnedBytesLimitIsEnforced() throws IOException {
        openStore();
        writeBatch(1, 10);
        long segBytes;
        try (Stream<Path> files = Files.list(dir)) {
            segBytes = files.filter(p -> p.getFileName().toString().matches("seg-\\d+\\.ccs"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).sum();
        }
        store.close();
        log.info("[limit] reopening with maxPinnedBytes={}", segBytes);
        openStore(new SnapshotRegistry.SnapshotLimits(8, segBytes));

        VersionSnapshot first = store.openSnapshot(); // pins exactly segBytes
        writeBatch(11, 20); // a second segment the next snapshot would also pin
        StoreException e = assertThrows(StoreException.class, store::openSnapshot);
        assertEquals(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED, e.code());
        log.info("[limit] pinned-bytes overflow rejected: code={} msg='{}'", e.code(), e.getMessage());
        first.close();
    }

    @Test
    void releaseReclaimsDiskHeldByPinnedSegments() throws IOException {
        openStore();
        writeBatch(1, 10);
        VersionSnapshot snap = store.openSnapshot();
        writeBatch(11, 20);
        store.compact(); // old segments dropped from manifest; seg-1 still pinned
        assertEquals(2, segmentFileCount(),
                "pinned segment must survive compaction reclamation");
        log.info("[reclaim] pinned segment kept during compaction, files={}", segmentFileCount());

        snap.close();
        store.openSnapshot().close(); // triggers the drain again; also proves slots are free
        assertEquals(1, segmentFileCount(),
                "once the last pin closes, the deferred segment file must be deleted");
        assertEquals(0, store.snapshotPinnedBytes());
        log.info("[reclaim] PASS: deferred segment reclaimed after snapshot close, files={}",
                segmentFileCount());
    }

    @Test
    void missingPinnedSegmentFailsWithSnapshotExpired() throws IOException {
        openStore();
        writeBatch(1, 5);
        VersionSnapshot snap = store.openSnapshot();
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.getFileName().toString().matches("seg-\\d+\\.ccs"))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        StoreException e = assertThrows(StoreException.class, snap::read);
        assertEquals(ErrorCode.SNAPSHOT_EXPIRED, e.code());
        log.info("[snapshot] lost pinned segment: code={} | basis: never silently partial", e.code());
        snap.close();
    }

    @Test
    void snapshotSeesSchemaAndDefaultsOfItsOwnVersion() {
        openStore();
        writeBatch(1, 5);
        VersionSnapshot snap = store.openSnapshot();
        store.addColumn("score", ColumnType.INT);
        List<Map<String, Object>> evolved = new ArrayList<>();
        for (long i = 6; i <= 7; i++) {
            evolved.add(Map.of("id", i, "name", "user-" + i, "score", (int) i));
        }
        store.write(new Batch(evolved)); // rows now carry the new column
        List<Map<String, Object>> rows = snap.read();
        assertEquals(5, rows.size());
        assertTrue(rows.stream().noneMatch(r -> r.containsKey("score")),
                "snapshot schema is the one at open time");
        assertEquals(2, snap.schema().columnCount());
        snap.close();
        assertEquals(3, store.readSnapshot().schema().columnCount());
        log.info("[snapshot] PASS: schema pinned at open version; live schema evolved independently");
    }
}
