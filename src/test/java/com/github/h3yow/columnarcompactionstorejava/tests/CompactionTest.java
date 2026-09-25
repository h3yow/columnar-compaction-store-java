package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.ColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionStatus;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 压实幂等性：合并前后逻辑数据一致；重复触发分别得到 COMPACTED / NOTHING_TO_COMPACT，
 * 不得产生重复段或重复计数。
 */
class CompactionTest {

    private static Row row(long id, String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        return new Row(m);
    }

    private long countDatFiles(Path dir) throws Exception {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().matches("seg-\\d+\\.dat")).count();
        }
    }

    @Test
    void repeatedCompactionDoesNotDuplicateDataOrSegments(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir).withCompactionThreshold(2);
        DiskColumnarStore store = DiskColumnarStore.open(cfg);

        List<String> expectedNames = new ArrayList<>();
        int batches = 5;
        for (int b = 0; b < batches; b++) {
            WriteBatch wb = new WriteBatch("c" + b, List.of("id", "name"), List.of(
                    row(b * 2L, "u" + b + "-0"),
                    row(b * 2L + 1, "u" + b + "-1")));
            assertTrue(store.write(wb).accepted());
            expectedNames.add("u" + b + "-0");
            expectedNames.add("u" + b + "-1");
        }

        ScanResult before = store.scan();
        assertEquals(10, before.rows().size());
        assertEquals(5, before.segmentIds().size());
        long filesBefore = countDatFiles(dir);
        assertEquals(5L, filesBefore);

        // 第一次压实：5 段 -> 1 段，行数不变
        CompactionResult c1 = store.compact();
        System.out.println("[压实 #1] status=" + c1.status() + " segments " + c1.segmentsBefore()
                + "->" + c1.segmentsAfter() + " rows " + c1.rowsBefore() + "->" + c1.rowsAfter()
                + " newSeg=" + c1.newSegmentId() + " 依据=" + c1.detail());
        assertEquals(CompactionStatus.COMPACTED, c1.status());
        assertEquals(5, c1.segmentsBefore());
        assertEquals(1, c1.segmentsAfter());
        assertEquals(10L, c1.rowsBefore());
        assertEquals(10L, c1.rowsAfter());

        // 物理文件应只剩 1 个（旧段引用归零后删除）
        long filesAfter = countDatFiles(dir);
        System.out.println("[文件计数] 压实前 seg 文件=" + filesBefore + " 压实后=" + filesAfter);
        assertEquals(1L, filesAfter, "旧段必须在无在途读者时被删除");

        // 第二次压实：无东西可压
        CompactionResult c2 = store.compact();
        System.out.println("[压实 #2] status=" + c2.status() + " 依据=" + c2.detail());
        assertEquals(CompactionStatus.NOTHING_TO_COMPACT, c2.status());

        // 第三次压实仍然 NOTHING_TO_COMPACT，段与行数都不变
        CompactionResult c3 = store.compact();
        assertEquals(CompactionStatus.NOTHING_TO_COMPACT, c3.status());

        ScanResult after = store.scan();
        assertEquals(10, after.rows().size(), "压实后行数不得变化");
        assertEquals(1, after.segmentIds().size());
        assertNotEquals(before.segmentIds().get(0), after.segmentIds().get(0), "应产生新段 id");

        List<String> actualNames = new ArrayList<>();
        after.rows().forEach(r -> actualNames.add((String) r.values().get("name")));
        assertEquals(expectedNames, actualNames, "压实前后逻辑数据必须一致且有序");

        // 再写入后压实：2 段 -> 1 段，不重复计数
        assertTrue(store.write(new WriteBatch("c6", List.of("id", "name"),
                List.of(row(100L, "new-0")))).accepted());
        CompactionResult c4 = store.compact();
        System.out.println("[压实 #4（写入新段后）] status=" + c4.status()
                + " segments " + c4.segmentsBefore() + "->" + c4.segmentsAfter()
                + " rows " + c4.rowsBefore() + "->" + c4.rowsAfter());
        assertEquals(CompactionStatus.COMPACTED, c4.status());
        assertEquals(2, c4.segmentsBefore());
        assertEquals(1, c4.segmentsAfter());
        assertEquals(11L, c4.rowsAfter());
        ScanResult finalScan = store.scan();
        assertEquals(11, finalScan.rows().size());
        assertEquals(100L, finalScan.rows().get(10).values().get("id"));

        store.close();
    }

    @Test
    void compactionPreservesNewColumnNulls(@TempDir Path dir) {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (DiskColumnarStore store = DiskColumnarStore.open(cfg)) {
            assertTrue(store.write(new WriteBatch("old1", List.of("id"), List.of(
                    new Row(Map.of("id", 1L))))).accepted());
            assertTrue(store.write(new WriteBatch("old2", List.of("id", "score"), List.of(
                    new Row(Map.of("id", 2L, "score", 0.5))))).accepted());

            CompactionResult c = store.compact();
            assertEquals(CompactionStatus.COMPACTED, c.status());
            ScanResult scan = store.scan();
            assertEquals(2, scan.rows().size());
            assertEquals(null, scan.rows().get(0).values().get("score"),
                    "压实后旧行的新列缺省仍必须为 null");
            assertEquals(0.5, scan.rows().get(1).values().get("score"));
        }
    }
}
