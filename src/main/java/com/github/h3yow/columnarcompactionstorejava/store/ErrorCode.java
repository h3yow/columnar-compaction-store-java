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
    /** Too many version snapshots are open at the same time. */
    SNAPSHOT_LIMIT_EXCEEDED,
    /** Open snapshots would pin more bytes than the configured snapshot budget. */
    SNAPSHOT_BYTES_LIMIT_EXCEEDED,
    /** Requested version is older than the retention window and has been expired. */
    SNAPSHOT_VERSION_EXPIRED,
    /** A snapshot can no longer be served consistently (its data was externally removed). */
    SNAPSHOT_DATA_UNAVAILABLE,
    /** Read or release attempted on a closed version snapshot. */
    SNAPSHOT_CLOSED,
    /** Retention cleanup was interrupted before its reclaim phase finished; safe to retry. */
    CLEANUP_INTERRUPTED
}
