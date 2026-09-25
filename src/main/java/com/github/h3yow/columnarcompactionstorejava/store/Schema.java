package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable ordered list of columns with a monotonically increasing version.
 *
 * <p>Evolution rules:
 * <ul>
 *   <li>Columns may only be <b>appended</b>; the new column's default value
 *       (type-specific zero value, see {@link #defaultValueOf}) is used for
 *       rows written before the column existed. This semantics is fixed and
 *       stable across versions.</li>
 *   <li>Renaming or dropping a column is rejected with
 *       {@link ErrorCode#SCHEMA_INCOMPATIBLE} so readers can never silently
 *       misalign columns.</li>
 * </ul>
 */
public final class Schema {

    private final int version;
    private final List<ColumnDef> columns;
    private final Map<String, Integer> indexByName;

    private Schema(int version, List<ColumnDef> columns) {
        this.version = version;
        this.columns = List.copyOf(columns);
        Map<String, Integer> idx = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            idx.put(columns.get(i).name(), i);
        }
        this.indexByName = Collections.unmodifiableMap(idx);
    }

    public static Schema of(List<ColumnDef> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH, "schema must contain at least one column");
        }
        return new Schema(1, columns);
    }

    /** Reconstructs a schema with an explicit version (used when loading a manifest). */
    public static Schema of(int version, List<ColumnDef> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new StoreException(ErrorCode.MALFORMED_BATCH, "schema must contain at least one column");
        }
        return new Schema(version, columns);
    }

    public int version() {
        return version;
    }

    public List<ColumnDef> columns() {
        return columns;
    }

    public boolean contains(String name) {
        return indexByName.containsKey(name);
    }

    public int indexOf(String name) {
        Integer i = indexByName.get(name);
        return i == null ? -1 : i;
    }

    public ColumnDef column(int index) {
        return columns.get(index);
    }

    public int columnCount() {
        return columns.size();
    }

    /**
     * Returns a new schema with one column appended. The default value for the
     * new column is the deterministic type zero value.
     */
    public Schema addColumn(String name, ColumnType type) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        if (indexByName.containsKey(name)) {
            throw new StoreException(ErrorCode.SCHEMA_INCOMPATIBLE,
                    "column '" + name + "' already exists; re-add would be a rename/retype");
        }
        List<ColumnDef> next = new ArrayList<>(columns);
        next.add(new ColumnDef(name, type));
        return new Schema(version + 1, next);
    }

    /**
     * Validates that {@code evolved} is a legal successor of this schema:
     * every existing column must still exist at the same position with the
     * same name and type (i.e. only appends are allowed).
     *
     * @throws StoreException with {@link ErrorCode#SCHEMA_INCOMPATIBLE} on rename/drop/retype
     */
    public void assertCompatibleSuccessor(Schema evolved) {
        for (int i = 0; i < columns.size(); i++) {
            if (i >= evolved.columns.size()) {
                throw new StoreException(ErrorCode.SCHEMA_INCOMPATIBLE,
                        "column '" + columns.get(i).name() + "' was dropped");
            }
            ColumnDef before = columns.get(i);
            ColumnDef after = evolved.columns.get(i);
            if (!before.name().equals(after.name())) {
                throw new StoreException(ErrorCode.SCHEMA_INCOMPATIBLE,
                        "column at position " + i + " renamed from '" + before.name()
                                + "' to '" + after.name() + "'; renames are forbidden");
            }
            if (before.type() != after.type()) {
                throw new StoreException(ErrorCode.SCHEMA_INCOMPATIBLE,
                        "column '" + before.name() + "' retyped from " + before.type()
                                + " to " + after.type());
            }
        }
    }

    /**
     * Deterministic default value for a column type, used to materialize
     * values of columns that did not exist when a row was written.
     */
    public static Object defaultValueOf(ColumnType type) {
        return switch (type) {
            case INT -> 0;
            case LONG -> 0L;
            case DOUBLE -> 0.0d;
            case BOOL -> false;
            case STRING -> "";
        };
    }

    @Override
    public String toString() {
        return "Schema{v" + version + ", columns=" + columns + "}";
    }
}
