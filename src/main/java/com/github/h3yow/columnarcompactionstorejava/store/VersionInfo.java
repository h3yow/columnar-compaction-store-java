package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.List;

/**
 * Immutable descriptor of one historical store version: the state of the
 * store exactly when a write batch became visible. Versions are monotonically
 * numbered and never change; they disappear only when retention expires them.
 *
 * @param versionId       monotonic version id
 * @param createdAtMillis wall-clock time when the version became visible
 * @param compactionGeneration compaction generation at that point
 * @param schemaVersion   schema version at that point
 * @param columnCount     column count at that point (schema was append-only)
 * @param segments        live segment file names at that point, in order
 */
public record VersionInfo(long versionId,
                          long createdAtMillis,
                          long compactionGeneration,
                          int schemaVersion,
                          int columnCount,
                          List<String> segments) {

    public VersionInfo {
        segments = List.copyOf(segments);
    }

    /** Skeleton factory; real implementation may validate later. */
    static VersionInfo of(long versionId, long createdAtMillis, long compactionGeneration,
                          int schemaVersion, int columnCount, List<String> segments) {
        return new VersionInfo(versionId, createdAtMillis, compactionGeneration,
                schemaVersion, columnCount, segments);
    }
}
