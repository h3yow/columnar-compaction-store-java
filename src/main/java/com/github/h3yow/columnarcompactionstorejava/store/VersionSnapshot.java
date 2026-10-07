package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.List;
import java.util.Map;

/**
 * User handle to one explicitly opened, immutable version snapshot.
 *
 * <p>Every {@link #read()} returns exactly the logical data visible when the
 * snapshot was opened, no matter how many writes, compactions or expiration
 * cleanups happen afterwards. The snapshot pins its underlying segments for
 * as long as it stays open; {@link #close()} (idempotent) releases them and
 * hands the pinned capacity back.
 *
 * <p>Read failures are distinguishable:
 * {@link ErrorCode#SNAPSHOT_DATA_UNAVAILABLE} when the underlying files can no
 * longer guarantee consistency (e.g. externally deleted), never silent empty
 * or misaligned data. Operations on a released snapshot fail with
 * {@link ErrorCode#SNAPSHOT_CLOSED}.
 */
public final class VersionSnapshot implements AutoCloseable {

    private final long snapshotId;
    private final VersionInfo version;
    private final ColumnarStore store;
    private volatile boolean closed;

    VersionSnapshot(long snapshotId, VersionInfo version, ColumnarStore store) {
        this.snapshotId = snapshotId;
        this.version = version;
        this.store = store;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long versionId() {
        return version.versionId();
    }

    public VersionInfo versionInfo() {
        return version;
    }

    public boolean isClosed() {
        return closed;
    }

    void markClosed() {
        closed = true;
    }

    /** Re-reads the immutable view; stable for the snapshot's whole lifetime. */
    public ColumnarStore.Snapshot read() {
        if (closed) {
            throw new StoreException(ErrorCode.SNAPSHOT_CLOSED,
                    "snapshot #" + snapshotId + " of v" + version.versionId() + " is closed");
        }
        return store.readVersion(this);
    }

    /** Idempotent release of the snapshot and its pinned segments. */
    @Override
    public void close() {
        store.releaseSnapshot(this);
    }

    /** Segments backing this snapshot, in read order. */
    List<String> segments() {
        return version.segments();
    }

    /** Convenience: rows of this snapshot in write order. */
    public List<Map<String, Object>> readRows() {
        return read().rows();
    }
}
