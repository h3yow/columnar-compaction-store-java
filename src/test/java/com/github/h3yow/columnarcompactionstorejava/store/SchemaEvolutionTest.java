package com.github.h3yow.columnarcompactionstorejava.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Schema evolution: appends carry deterministic defaults; rename/drop/retype
 * are rejected so readers never silently misalign columns.
 */
class SchemaEvolutionTest {

    private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionTest.class);

    @TempDir
    Path dir;

    private ColumnarStore store;

    @BeforeEach
    void openStore() {
        store = ColumnarStore.open(dir, Schema.of(List.of(
                new ColumnDef("id", ColumnType.LONG))), new ResourceLimiter(1 << 20, 8));
    }

    @Test
    void appendedColumnReadsBackWithDeterministicDefault() {
        store.write(new Batch(List.of(Map.of("id", 1L), Map.of("id", 2L))));
        store.addColumn("tag", ColumnType.STRING);
        store.addColumn("vip", ColumnType.BOOL);

        // new rows must carry the new fields
        store.write(new Batch(List.of(Map.of("id", 3L, "tag", "c", "vip", true))));

        ColumnarStore.Snapshot snap = store.readSnapshot();
        log.info("[append] schema={} rows={} | basis: pre-append rows get type defaults "
                + "(STRING->\"\", BOOL->false)", snap.schema(), snap.rows());
        assertEquals(Map.of("id", 1L, "tag", "", "vip", false), snap.rows().get(0));
        assertEquals(Map.of("id", 2L, "tag", "", "vip", false), snap.rows().get(1));
        assertEquals(Map.of("id", 3L, "tag", "c", "vip", true), snap.rows().get(2));
    }

    @Test
    void defaultsAreStableAcrossCompaction() {
        store.write(new Batch(List.of(Map.of("id", 1L))));
        store.addColumn("score", ColumnType.DOUBLE);
        store.write(new Batch(List.of(Map.of("id", 2L, "score", 3.5))));
        store.compact();
        ColumnarStore.Snapshot snap = store.readSnapshot();
        log.info("[append-compact] rows={} | basis: default 0.0 survives compaction", snap.rows());
        assertEquals(Map.of("id", 1L, "score", 0.0), snap.rows().get(0));
        assertEquals(Map.of("id", 2L, "score", 3.5), snap.rows().get(1));
    }

    @Test
    void renameIsRejected() {
        Schema renamed = Schema.of(List.of(new ColumnDef("identifier", ColumnType.LONG)));
        StoreException e = assertThrows(StoreException.class, () -> store.evolveTo(renamed));
        log.info("[rename] proposed={} rejected code={} msg='{}' | basis: position-0 name differs",
                renamed, e.code(), e.getMessage());
        assertEquals(ErrorCode.SCHEMA_INCOMPATIBLE, e.code());
    }

    @Test
    void dropIsRejected() {
        store.addColumn("tag", ColumnType.STRING);
        Schema dropped = Schema.of(List.of(new ColumnDef("id", ColumnType.LONG)));
        StoreException e = assertThrows(StoreException.class, () -> store.evolveTo(dropped));
        log.info("[drop] proposed={} rejected code={} msg='{}' | basis: column count shrinks",
                dropped, e.code(), e.getMessage());
        assertEquals(ErrorCode.SCHEMA_INCOMPATIBLE, e.code());
    }

    @Test
    void retypeIsRejected() {
        Schema retyped = Schema.of(List.of(new ColumnDef("id", ColumnType.STRING)));
        StoreException e = assertThrows(StoreException.class, () -> store.evolveTo(retyped));
        log.info("[retype] proposed={} rejected code={} msg='{}'", retyped, e.code(), e.getMessage());
        assertEquals(ErrorCode.SCHEMA_INCOMPATIBLE, e.code());
    }

    @Test
    void reAddExistingNameIsRejectedAsRename() {
        StoreException e = assertThrows(StoreException.class,
                () -> store.addColumn("id", ColumnType.STRING));
        log.info("[re-add] rejected code={} msg='{}'", e.code(), e.getMessage());
        assertEquals(ErrorCode.SCHEMA_INCOMPATIBLE, e.code());
    }

    @Test
    void schemaSurvivesReopen() {
        store.write(new Batch(List.of(Map.of("id", 7L))));
        store.addColumn("tag", ColumnType.STRING);
        store.close();

        ColumnarStore reopened = ColumnarStore.open(dir, Schema.of(List.of(
                new ColumnDef("id", ColumnType.LONG))), new ResourceLimiter(1 << 20, 8));
        ColumnarStore.Snapshot snap = reopened.readSnapshot();
        log.info("[reopen] schema={} rows={} | basis: manifest persists schema version",
                snap.schema(), snap.rows());
        assertEquals(2, snap.schema().version());
        assertEquals(Map.of("id", 7L, "tag", ""), snap.rows().get(0));
    }
}
