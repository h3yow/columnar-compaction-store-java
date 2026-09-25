package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Malformed batches must be rejected with distinguishable reasons.
 * Each case logs the input batch and the decision basis (expected vs actual code).
 */
class BatchValidationTest {

    private static final Logger log = LoggerFactory.getLogger(BatchValidationTest.class);

    @TempDir
    Path dir;

    private ColumnarStore store;

    @BeforeEach
    void openStore() {
        Schema schema = Schema.of(List.of(
                new ColumnDef("id", ColumnType.LONG),
                new ColumnDef("name", ColumnType.STRING),
                new ColumnDef("score", ColumnType.DOUBLE)));
        store = ColumnarStore.open(dir, schema, new ResourceLimiter(1 << 20, 8));
    }

    private void expectReject(String caseName, Batch batch, ErrorCode expected) {
        log.info("[case={}] input batch={} | decision basis: expect {} because of the documented rule",
                caseName, batch, expected);
        StoreException e = assertThrows(StoreException.class, () -> store.write(batch));
        log.info("[case={}] actual code={} message='{}' | match={}",
                caseName, e.code(), e.getMessage(), e.code() == expected);
        assertEquals(expected, e.code());
    }

    @Test
    void rejectsNullRowList() {
        expectReject("null-rows", new Batch(null), ErrorCode.MALFORMED_BATCH);
    }

    @Test
    void rejectsEmptyBatch() {
        expectReject("empty-batch", new Batch(List.of()), ErrorCode.MALFORMED_BATCH);
    }

    @Test
    void rejectsNullRow() {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(null);
        expectReject("null-row", new Batch(rows), ErrorCode.MALFORMED_BATCH);
    }

    @Test
    void rejectsUnknownField() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        row.put("name", "alice");
        row.put("score", 9.5);
        row.put("ghost", "boo"); // not in schema
        expectReject("unknown-field", new Batch(List.of(row)), ErrorCode.UNKNOWN_FIELD);
    }

    @Test
    void rejectsMissingField() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        row.put("name", "alice");
        // score absent
        expectReject("missing-field", new Batch(List.of(row)), ErrorCode.MISSING_FIELD);
    }

    @Test
    void rejectsNullFieldValue() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        row.put("name", null);
        row.put("score", 9.5);
        expectReject("null-value", new Batch(List.of(row)), ErrorCode.MISSING_FIELD);
    }

    @Test
    void rejectsTypeMismatch() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1); // Integer where LONG expected
        row.put("name", "alice");
        row.put("score", 9.5);
        expectReject("type-mismatch", new Batch(List.of(row)), ErrorCode.TYPE_MISMATCH);
    }

    @Test
    void acceptsValidBatchAndReadsItBack() {
        Batch batch = new Batch(List.of(
                Map.of("id", 1L, "name", "alice", "score", 9.5),
                Map.of("id", 2L, "name", "bob", "score", 7.25)));
        log.info("[case=valid] input batch={} | decision basis: all fields present and typed", batch);
        store.write(batch);
        ColumnarStore.Snapshot snap = store.readSnapshot();
        log.info("[case=valid] snapshot rows={}", snap.rows());
        assertEquals(batch.rows(), snap.rows());
    }

    @Test
    void rejectedBatchLeavesNoVisibleRows() {
        Map<String, Object> bad = new HashMap<>();
        bad.put("id", 1L);
        bad.put("name", "alice");
        bad.put("score", "not-a-double");
        expectReject("atomicity", new Batch(List.of(bad)), ErrorCode.TYPE_MISMATCH);
        ColumnarStore.Snapshot snap = store.readSnapshot();
        log.info("[case=atomicity] rows after rejected write={} | basis: rejection is all-or-nothing",
                snap.rows());
        assertEquals(0, snap.rows().size());
    }
}
