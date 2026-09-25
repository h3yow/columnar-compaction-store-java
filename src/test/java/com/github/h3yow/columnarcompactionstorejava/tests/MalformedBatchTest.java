package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.ColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.model.BatchRejectReason;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 畸形 / 不合法批次的拒绝原因必须可区分，且批次被原子拒绝（无部分行写入）。
 * 每个用例均在日志（stdout）打印输入批次与判定依据。
 */
class MalformedBatchTest {

    private static Row row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return new Row(m);
    }

    private WriteResult attempt(ColumnarStore store, String label, WriteBatch batch) {
        System.out.println("---- 用例 " + label + " ----");
        System.out.println("[输入批次] id=" + (batch == null ? null : batch.batchId())
                + " columns=" + (batch == null ? null : batch.columns())
                + " rows=" + (batch == null ? null : batch.rows()));
        WriteResult r = store.write(batch);
        if (r.accepted()) {
            System.out.println("[判定结果] accepted 段=" + r.segmentId());
        } else {
            System.out.println("[判定结果] rejected 原因=" + r.rejection().reason()
                    + " 列=" + r.rejection().column()
                    + " 依据=" + r.rejection().detail());
        }
        return r;
    }

    @Test
    void allRejectionReasonsAreDistinguishable(@TempDir Path dir) {
        // 极小内存上限用于触发 BATCH_TOO_LARGE
        StoreConfig cfg = StoreConfig.defaults(dir).withMaxWriteBatchBytes(50L);
        try (ColumnarStore store = DiskColumnarStore.open(cfg)) {
            WriteResult base = store.write(new WriteBatch("seed", List.of("id", "name", "flag"), List.of(
                    row("id", 1L, "name", "a", "flag", true))));
            assertEquals(true, base.accepted());

            WriteResult r;

            r = attempt(store, "空 batchId", new WriteBatch(" ", List.of("id"), List.of(row("id", 1L))));
            assertReject(r, BatchRejectReason.MALFORMED_BATCH);

            r = attempt(store, "空列头", new WriteBatch("x1", List.of(), List.of(row("id", 1L))));
            assertReject(r, BatchRejectReason.MALFORMED_BATCH);

            r = attempt(store, "重复列名", new WriteBatch("x2", List.of("id", "id"),
                    List.of(row("id", 1L))));
            assertReject(r, BatchRejectReason.MALFORMED_BATCH);

            r = attempt(store, "空行集合", new WriteBatch("x3", List.of("id"), List.of()));
            assertReject(r, BatchRejectReason.MALFORMED_BATCH);

            r = attempt(store, "null 行", new WriteBatch("x3b", List.of("id"),
                    java.util.Arrays.asList((Row) null)));
            assertReject(r, BatchRejectReason.MALFORMED_BATCH);

            r = attempt(store, "缺字段", new WriteBatch("x4", List.of("id", "name"),
                    List.of(row("id", 2L))));
            assertReject(r, BatchRejectReason.MISSING_FIELD);
            assertEquals("name", r.rejection().column());

            r = attempt(store, "未知字段", new WriteBatch("x5", List.of("id"),
                    List.of(row("id", 2L, "extra", 1L))));
            assertReject(r, BatchRejectReason.UNKNOWN_FIELD);

            r = attempt(store, "类型不匹配(long<-string)", new WriteBatch("x6", List.of("id"),
                    List.of(row("id", "abc"))));
            assertReject(r, BatchRejectReason.TYPE_MISMATCH);
            assertEquals("id", r.rejection().column());

            r = attempt(store, "类型不匹配(string<-boolean)", new WriteBatch("x7", List.of("id", "name"),
                    List.of(row("id", 4L, "name", true))));
            assertReject(r, BatchRejectReason.TYPE_MISMATCH);
            assertEquals("name", r.rejection().column());

            r = attempt(store, "类型不匹配(boolean<-long)", new WriteBatch("x7b", List.of("id", "flag"),
                    List.of(row("id", 5L, "flag", 1L))));
            assertReject(r, BatchRejectReason.TYPE_MISMATCH);
            assertEquals("flag", r.rejection().column());

            r = attempt(store, "重复 batchId", new WriteBatch("seed", List.of("id", "name", "flag"),
                    List.of(row("id", 9L, "name", "dup", "flag", false))));
            assertReject(r, BatchRejectReason.DUPLICATE_BATCH_ID);

            r = attempt(store, "批次过大", new WriteBatch("x8", List.of("id", "name"),
                    List.of(row("id", 5L, "name", "this-is-a-fairly-long-string-value-xxxxxx"))));
            assertReject(r, BatchRejectReason.BATCH_TOO_LARGE);

            // 全部拒绝后，数据仍只有 seed 一行
            assertEquals(1, store.scan().rows().size(), "被拒绝批次不得写入任何行");
        }
    }

    private void assertReject(WriteResult r, BatchRejectReason expected) {
        assertFalse(r.accepted(), "期望被拒绝: " + expected);
        assertEquals(expected, r.rejection().reason());
    }
}
