package com.github.h3yow.columnarcompactionstorejava.store;

/**
 * Explicit upper bounds on resources held by concurrently open version
 * snapshots:
 * <ul>
 *   <li>{@code maxOpenSnapshots} — number of simultaneously open snapshots</li>
 *   <li>{@code maxPinnedBytes} — total disk bytes of the union of segments
 *       pinned by open snapshots (shared segments are counted once)</li>
 * </ul>
 * Over-limit opens wait for the configured timeout and are then rejected with
 * a distinguishable code ({@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED} or
 * {@link ErrorCode#SNAPSHOT_BYTES_LIMIT_EXCEEDED}); closing snapshots hands
 * the capacity back.
 */
public record SnapshotLimits(int maxOpenSnapshots, long maxPinnedBytes) {

    public SnapshotLimits {
        if (maxOpenSnapshots <= 0 || maxPinnedBytes <= 0) {
            throw new IllegalArgumentException("snapshot limits must be positive");
        }
    }

    /** Skeleton defaults; finalized in the wiring step. */
    public static SnapshotLimits defaults() {
        return new SnapshotLimits(16, Long.MAX_VALUE);
    }
}
