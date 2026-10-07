package com.github.h3yow.columnarcompactionstorejava.store;

/** Machine-distinguishable failure reasons for store operations. */
public enum ErrorCode {
    /** A required field is absent from a row of the batch. */
    MISSING_FIELD,
    /** A field value does not match the declared column type. */
    TYPE_MISMATCH,
    /** The batch contains a field not present in the schema. */
    UNKNOWN_FIELD,
    /** The batch structure itself is incomplete/invalid (e.g. null rows, truncated data). */
    MALFORMED_BATCH,
    /** Schema change attempts to rename or drop a column, which is forbidden. */
    SCHEMA_INCOMPATIBLE,
    /** Memory or open-file budget exceeded; caller may retry later. */
    RESOURCE_LIMIT_EXCEEDED,
    /** A segment file on disk is corrupted (bad magic, length or checksum). */
    CORRUPT_SEGMENT,
    /** Operation attempted on a closed store. */
    STORE_CLOSED,
    /** Too many concurrent snapshots or too much snapshot-pinned data; caller may retry later. */
    SNAPSHOT_LIMIT_EXCEEDED,
    /** Operation attempted on a snapshot that was already released. */
    SNAPSHOT_CLOSED,
    /** The snapshot can no longer be served consistently (its pinned data is gone). */
    SNAPSHOT_EXPIRED
}
