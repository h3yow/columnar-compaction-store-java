package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resource budgets reject over-limit operations with a distinguishable code,
 * and corrupted segment files are reported as CORRUPT_SEGMENT.
 */
class ResourceAndCorruptionTest {

    private static final Logger log = LoggerFactory.getLogger(ResourceAndCorruptionTest.class);

    private static final Schema SCHEMA = Schema.of(List.of(
            new ColumnDef("id", ColumnType.LONG),
            new ColumnDef("name", ColumnType.STRING)));

    @Test
    void overBudgetWriteIsRejectedWithResourceCode(@TempDir Path dir) {
        // budget smaller than one row's estimate
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(8, 4), 0);
        Batch batch = new Batch(List.of(Map.of("id", 1L, "name", "alice")));
        StoreException e = assertThrows(StoreException.class, () -> store.write(batch));
        log.info("[mem-limit] batch={} rejected code={} msg='{}' | basis: estimate {} > budget 8",
                batch, e.code(), e.getMessage(), "row overhead");
        assertEquals(ErrorCode.RESOURCE_LIMIT_EXCEEDED, e.code());
        assertEquals(0, store.readSnapshot().rows().size(), "rejected write leaves nothing visible");
    }

    @Test
    void exhaustedMemoryBudgetBlocksSecondWrite(@TempDir Path dir) {
        // enough for exactly one small batch, not two in flight; wait=0 => immediate reject
        ResourceLimiter limiter = new ResourceLimiter(256, 8);
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, limiter, 0);
        Batch batch = new Batch(List.of(Map.of("id", 1L, "name", "a")));
        store.write(batch); // fits (grant released after write)
        log.info("[mem-limit] inUse after released write={} | basis: grants are released on completion",
                limiter.memoryInUse());
        assertEquals(0, limiter.memoryInUse(), "memory grant must be released after write");

        // now hold the whole budget externally: the next write must be rejected
        try (ResourceLimiter.MemoryGrant held = limiter.acquireMemory(256, 0)) {
            StoreException e = assertThrows(StoreException.class, () -> store.write(batch));
            log.info("[mem-limit] write under held budget rejected code={} msg='{}'",
                    e.code(), e.getMessage());
            assertEquals(ErrorCode.RESOURCE_LIMIT_EXCEEDED, e.code());
        }
    }

    @Test
    void exhaustedFilePermitsRejectWithResourceCode(@TempDir Path dir) {
        ResourceLimiter limiter = new ResourceLimiter(1 << 20, 1);
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, limiter, 0);
        try (ResourceLimiter.FilePermit held = limiter.acquireFile(0)) {
            Batch batch = new Batch(List.of(Map.of("id", 1L, "name", "a")));
            StoreException e = assertThrows(StoreException.class, () -> store.write(batch));
            log.info("[file-limit] write with no file permit rejected code={} msg='{}'",
                    e.code(), e.getMessage());
            assertEquals(ErrorCode.RESOURCE_LIMIT_EXCEEDED, e.code());
        }
    }

    @Test
    void corruptedSegmentIsDetectedByChecksum(@TempDir Path dir) throws IOException {
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(1 << 20, 8));
        store.write(new Batch(List.of(Map.of("id", 1L, "name", "alice"))));
        String segment = store.currentManifest().liveSegments().get(0);
        Path segPath = dir.resolve(segment);

        // flip a payload byte (after the 20-byte header, before the CRC trailer)
        try (RandomAccessFile raf = new RandomAccessFile(segPath.toFile(), "rw")) {
            long pos = Math.min(40, raf.length() - 9);
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
            log.info("[corrupt] flipped byte at offset {} of {}", pos, segment);
        }
        StoreException e = assertThrows(StoreException.class, store::readSnapshot);
        log.info("[corrupt] read rejected code={} msg='{}' | basis: CRC mismatch", e.code(), e.getMessage());
        assertEquals(ErrorCode.CORRUPT_SEGMENT, e.code());
    }

    @Test
    void truncatedSegmentIsDetected(@TempDir Path dir) throws IOException {
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(1 << 20, 8));
        store.write(new Batch(List.of(Map.of("id", 1L, "name", "alice"))));
        Path segPath = dir.resolve(store.currentManifest().liveSegments().get(0));
        long original = Files.size(segPath);
        try (RandomAccessFile raf = new RandomAccessFile(segPath.toFile(), "rw")) {
            raf.setLength(original / 2);
        }
        StoreException e = assertThrows(StoreException.class, store::readSnapshot);
        log.info("[truncated] read rejected code={} msg='{}' | basis: structural length check",
                e.code(), e.getMessage());
        assertEquals(ErrorCode.CORRUPT_SEGMENT, e.code());
    }

    @Test
    void orphanTmpFilesAreCleanedOnReopen(@TempDir Path dir) throws IOException {
        ColumnarStore store = ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(1 << 20, 8));
        store.write(new Batch(List.of(Map.of("id", 1L, "name", "alice"))));
        store.close();
        // simulate crash leftovers: a tmp file and an unreferenced segment
        Files.writeString(dir.resolve("seg-999999.ccs.tmp"), "half-written");
        Files.writeString(dir.resolve("seg-999998.ccs"), "orphan");

        ColumnarStore reopened = ColumnarStore.open(dir, SCHEMA, new ResourceLimiter(1 << 20, 8));
        List<Map<String, Object>> rows = reopened.readSnapshot().rows();
        long leftovers;
        try (var files = Files.list(dir)) {
            leftovers = files.filter(p -> p.getFileName().toString().contains("999")).count();
        }
        log.info("[orphan] rows={} leftover 999-files={} | basis: open() reclaims unreferenced files",
                rows, leftovers);
        assertEquals(1, rows.size());
        assertEquals(0, leftovers);
        assertTrue(rows.get(0).containsKey("id"));
    }
}
