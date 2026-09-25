package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.ColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.model.BatchRejectReason;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 基础：批次写入、全量读取、行序号、schema 追加演进与缺省语义。
 * 每个用例都会在日志中打印输入批次与判定依据。
 */
class BasicReadWriteTest {

    private static Row row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return new Row(m);
    }

    @Test
    void writeAndReadSingleBatch(@TempDir Path dir) {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (ColumnarStore store = DiskColumnarStore.open(cfg)) {
            WriteBatch batch = new WriteBatch("b1", List.of("id", "name", "active"), List.of(
                    row("id", 1L, "name", "alice", "active", true),
                    row("id", 2L, "name", "bob", "active", false)));
            System.out.println("[输入批次] b1 columns=" + batch.columns() + " rows=" + batch.rows());

            WriteResult wr = store.write(batch);
            System.out.println("[判定依据] accepted=" + wr.accepted() + " segment=" + wr.segmentId()
                    + " firstSeq=" + wr.firstSeq() + " rows=" + wr.rowCount());
            assertTrue(wr.accepted(), "合法批次应被接受");
            assertEquals(0L, wr.firstSeq());
            assertEquals(2, wr.rowCount());

            ScanResult scan = store.scan();
            System.out.println("[读取快照] version=" + scan.manifestVersion()
                    + " snapshotSeq=" + scan.snapshotSeq() + " rows=" + scan.rows().size()
                    + " segments=" + scan.segmentIds());
            assertEquals(2, scan.rows().size());
            assertEquals(1L, scan.rows().get(0).values().get("id"));
            assertEquals("alice", scan.rows().get(0).values().get("name"));
            assertEquals(2L, scan.rows().get(1).values().get("id"));
            assertEquals(2L, scan.snapshotSeq());
        }
    }

    @Test
    void newColumnsDefaultToNullForOldRowsAndNoColumnShiftingOnRename(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (ColumnarStore store = DiskColumnarStore.open(cfg)) {
            WriteBatch b1 = new WriteBatch("b1", List.of("id", "name"), List.of(
                    row("id", 10L, "name", "carol"),
                    row("id", 11L, "name", "dave")));
            WriteResult r1 = store.write(b1);
            assertTrue(r1.accepted());

            // 第二批新增列 score；旧两行对该列必须稳定缺省为 null
            WriteBatch b2 = new WriteBatch("b2", List.of("id", "name", "score"), List.of(
                    row("id", 12L, "name", "erin", "score", 9.5)));
            System.out.println("[输入批次] b2（新增列 score）=" + b2.rows());
            WriteResult r2 = store.write(b2);
            System.out.println("[判定依据] accepted=" + r2.accepted() + " detail schemaVersion 写入段");
            assertTrue(r2.accepted());

            ScanResult scan = store.scan();
            System.out.println("[读取快照] schema=" + scan.schema().columns());
            assertEquals(3, scan.schema().columns().size());
            // 列按 id 对齐，而非按位置
            assertNull(scan.rows().get(0).values().get("score"), "旧行新列缺省必须为 null");
            assertNull(scan.rows().get(1).values().get("score"));
            assertEquals(9.5, scan.rows().get(2).values().get("score"));
            assertEquals(10L, scan.rows().get(0).values().get("id"));

            // 「改名」场景：以新名字 score2 出现 = 新列追加；旧列 score 原样保留，绝不静默错列
            WriteBatch b3 = new WriteBatch("b3", List.of("id", "name", "score2"), List.of(
                    row("id", 13L, "name", "frank", "score2", 1.0)));
            WriteResult r3 = store.write(b3);
            assertTrue(r3.accepted());
            ScanResult scan2 = store.scan();
            assertEquals(4, scan2.schema().columns().size());
            assertEquals("score", scan2.schema().byId(2).name());
            assertEquals("score2", scan2.schema().byId(3).name());
            assertNull(scan2.rows().get(3).values().get("score"));
            assertEquals(1.0, scan2.rows().get(3).values().get("score2"));
        }
    }

    @Test
    void rowsMaintainGlobalSequenceAcrossBatches(@TempDir Path dir) {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (ColumnarStore store = DiskColumnarStore.open(cfg)) {
            List<Long> allIds = new ArrayList<>();
            for (int b = 0; b < 4; b++) {
                WriteBatch wb = new WriteBatch("batch-" + b, List.of("n"), List.of(
                        row("n", (long) (b * 10 + 1)),
                        row("n", (long) (b * 10 + 2))));
                WriteResult wr = store.write(wb);
                assertTrue(wr.accepted());
                assertEquals(b * 2L, wr.firstSeq(), "firstSeq 必须跨批次连续");
            }
            ScanResult scan = store.scan();
            assertEquals(8, scan.rows().size());
            for (int i = 0; i < 8; i++) {
                allIds.add((Long) scan.rows().get(i).values().get("n"));
            }
            assertEquals(List.of(1L, 2L, 11L, 12L, 21L, 22L, 31L, 32L), allIds);
            assertEquals(8L, scan.snapshotSeq());
            assertNotNull(scan.segmentIds());
            assertEquals(4, scan.segmentIds().size());
        }
    }

    @Test
    void rejectedBatchMutatesNothing(@TempDir Path dir) {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (ColumnarStore store = DiskColumnarStore.open(cfg)) {
            WriteBatch good = new WriteBatch("ok", List.of("id"), List.of(row("id", 1L)));
            assertTrue(store.write(good).accepted());

            // 坏批次 1：对既有 LONG 列 id 写 String -> TYPE_MISMATCH，不得改变行数/schema
            WriteBatch bad = new WriteBatch("bad", List.of("id"), List.of(
                    row("id", "not-a-long")));
            WriteResult rr = store.write(bad);
            System.out.println("[输入批次] bad=" + bad.rows());
            System.out.println("[判定依据] 坏批次原因=" + rr.rejection().reason()
                    + " 列=" + rr.rejection().column() + " 细节=" + rr.rejection().detail());
            assertFalse(rr.accepted());
            assertEquals(BatchRejectReason.TYPE_MISMATCH, rr.rejection().reason());

            // 坏批次 2：行多了列头未声明的字段 -> UNKNOWN_FIELD
            WriteBatch bad2 = new WriteBatch("bad2", List.of("id"), List.of(
                    row("id", 2L, "ghost", "x")));
            WriteResult rr2 = store.write(bad2);
            System.out.println("[判定依据] 坏批次2原因=" + rr2.rejection().reason()
                    + " 细节=" + rr2.rejection().detail());
            assertFalse(rr2.accepted());
            assertEquals(BatchRejectReason.UNKNOWN_FIELD, rr2.rejection().reason());

            ScanResult scan = store.scan();
            assertEquals(1, scan.rows().size());
            assertEquals(1, scan.schema().columns().size(), "拒绝的批次不得新增列");
        }
    }
}
