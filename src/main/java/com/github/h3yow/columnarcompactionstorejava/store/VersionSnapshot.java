package com.github.h3yow.columnarcompactionstorejava.store;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * An explicitly opened, pinned version snapshot of the store.
 *
 * <p>Rows returned by {@link #read()} are always exactly the logical data
 * visible at the moment the snapshot was opened, no matter how many writes,
 * compactions or retention cleanups happened since: the snapshot pins one
 * immutable {@link Manifest} and the segment files it references cannot be
 * reclaimed while this snapshot is open.
 *
 * <p>Failure semantics (never a silent empty/partial result):
 * <ul>
 *   <li>reading a closed snapshot fails with {@link ErrorCode#SNAPSHOT_CLOSED};</li>
 *   <li>reading after the owning store was closed fails with
 *       {@link ErrorCode#STORE_CLOSED};</li>
 *   <li>if a pinned segment file unexpectedly vanished (e.g. external
 *       deletion) the read fails with {@link ErrorCode#SNAPSHOT_EXPIRED}.</li>
 * </ul>
 *
 * <p>{@link #close()} is idempotent and returns all pinned capacity to the
 * store's snapshot budget; segments no longer referenced by any manifest or
 * snapshot are reclaimed by the store shortly after.
 */
public final class VersionSnapshot implements AutoCloseable {

    private final SnapshotRegistry registry;
    private final SnapshotRegistry.Pin pin;
    private final Path dir;
    private final ResourceLimiter limiter;
    private final long resourceWaitMillis;
    private final BooleanSupplier storeOpen;
    private final Runnable onRelease;
    private boolean closed;

    VersionSnapshot(SnapshotRegistry registry, SnapshotRegistry.Pin pin,
                    Path dir, ResourceLimiter limiter, long resourceWaitMillis,
                    BooleanSupplier storeOpen, Runnable onRelease) {
        this.registry = registry;
        this.pin = pin;
        this.dir = dir;
        this.limiter = limiter;
        this.resourceWaitMillis = resourceWaitMillis;
        this.storeOpen = storeOpen;
        this.onRelease = onRelease;
    }

    /** Store data version this snapshot was opened at. */
    public long version() {
        return pin.manifest.version();
    }

    /** Schema as of {@link #version()}. */
    public Schema schema() {
        return pin.manifest.schema();
    }

    /**
     * All rows visible at {@link #version()}, in write order. Repeatable:
     * every call returns the same logical content for the life of the
     * snapshot.
     */
    public synchronized List<Map<String, Object>> read() {
        if (closed) {
            throw new StoreException(ErrorCode.SNAPSHOT_CLOSED,
                    "snapshot at version " + version() + " is already closed");
        }
        if (!storeOpen.getAsBoolean()) {
            throw new StoreException(ErrorCode.STORE_CLOSED,
                    "owning store is closed; snapshot at version " + version()
                            + " can no longer be served");
        }
        Schema schema = pin.manifest.schema();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String segmentName : pin.segments) {
            SegmentFormat.SegmentData data;
            try (ResourceLimiter.FilePermit file = limiter.acquireFile(resourceWaitMillis)) {
                data = SegmentFormat.read(dir.resolve(segmentName));
            } catch (StoreException e) {
                if (e.getCause() instanceof NoSuchFileException) {
                    // a pinned segment must never disappear; if it did we must
                    // not silently return partial data
                    throw new StoreException(ErrorCode.SNAPSHOT_EXPIRED,
                            "snapshot at version " + version() + " lost pinned segment "
                                    + segmentName + "; it can no longer be served consistently", e);
                }
                throw e;
            }
            for (Map<String, Object> row : data.rows()) {
                rows.add(ColumnarStore.fillDefaults(row, schema));
            }
        }
        return List.copyOf(rows);
    }

    /** Total bytes of the segment files this snapshot pins on disk. */
    public long pinnedBytes() {
        return pin.bytes;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    /** Releases the pin exactly once; further calls are no-ops. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        registry.release(pin);
        onRelease.run();
    }
}
