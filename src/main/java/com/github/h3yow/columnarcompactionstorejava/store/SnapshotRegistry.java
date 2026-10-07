package com.github.h3yow.columnarcompactionstorejava.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks open {@link VersionSnapshot}s and the segment files they pin.
 *
 * <p>Enforces explicit upper bounds on the number of concurrent snapshots
 * and the total bytes they pin. Acquisition waits up to a timeout for a slot
 * and is then rejected with {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED},
 * carrying which bound was hit in the message. Releasing a snapshot
 * decrements the per-segment reference counts; once a segment is pinned by
 * no snapshot and referenced by no manifest, its bytes are returned by the
 * store's reaper.
 *
 * <p>All state transitions are serialized on this object's monitor, so
 * acquire/release interleaved with cleanup and compaction can never
 * double-count or leak pins.
 */
final class SnapshotRegistry {

    /** Hard limits on concurrently open snapshots. */
    record SnapshotLimits(int maxSnapshots, long maxPinnedBytes) {
        SnapshotLimits {
            if (maxSnapshots <= 0 || maxPinnedBytes <= 0) {
                throw new IllegalArgumentException("snapshot limits must be positive");
            }
        }

        static SnapshotLimits defaults() {
            return new SnapshotLimits(64, 1L << 30);
        }
    }

    /** Handle for one registered snapshot's pins. */
    static final class Pin {
        final Manifest manifest;
        final List<String> segments;
        final long bytes;
        boolean released;

        Pin(Manifest manifest, List<String> segments, long bytes) {
            this.manifest = manifest;
            this.segments = segments;
            this.bytes = bytes;
        }
    }

    private final Path dir;
    private final SnapshotLimits limits;
    /** segment name -> on-disk size, for every segment with refcount > 0. */
    private final Map<String, Long> pinnedSizes = new HashMap<>();
    private final Map<String, Integer> refcounts = new HashMap<>();
    private int openCount;
    private long pinnedBytes;

    SnapshotRegistry(Path dir, SnapshotLimits limits) {
        this.dir = dir;
        this.limits = limits;
    }

    /**
     * Registers a snapshot pinning every segment of {@code manifest}, waiting
     * up to {@code waitMillis} for capacity.
     *
     * @throws StoreException with {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED}
     *         if the snapshot-count or pinned-bytes bound cannot be satisfied
     *         within the wait
     */
    Pin acquire(Manifest manifest, long waitMillis) {
        List<String> names = manifest.liveSegments();
        Map<String, Long> sizes = new HashMap<>(names.size() * 2);
        for (String name : names) {
            sizes.put(name, fileSize(dir.resolve(name)));
        }
        synchronized (this) {
            long newBytes = 0;
            for (Map.Entry<String, Long> e : sizes.entrySet()) {
                if (!refcounts.containsKey(e.getKey())) {
                    newBytes += e.getValue();
                }
            }
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(waitMillis);
            while (openCount >= limits.maxSnapshots()
                    || pinnedBytes + newBytes > limits.maxPinnedBytes()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new StoreException(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED,
                            "snapshot limit exceeded: open=" + openCount + "/"
                                    + limits.maxSnapshots() + ", pinnedBytes=" + pinnedBytes
                                    + " (+" + newBytes + ")/" + limits.maxPinnedBytes());
                }
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StoreException(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED,
                            "interrupted while waiting for a snapshot slot");
                }
            }
            for (Map.Entry<String, Long> e : sizes.entrySet()) {
                refcounts.merge(e.getKey(), 1, Integer::sum);
                pinnedSizes.putIfAbsent(e.getKey(), e.getValue());
            }
            openCount++;
            pinnedBytes += newBytes;
            return new Pin(manifest, List.copyOf(names),
                    sizes.values().stream().mapToLong(Long::longValue).sum());
        }
    }

    /** Releases a pin exactly once; later calls are no-ops. */
    void release(Pin pin) {
        synchronized (this) {
            if (pin.released) {
                return;
            }
            pin.released = true;
            for (String name : pin.segments) {
                int count = refcounts.getOrDefault(name, 0) - 1;
                if (count <= 0) {
                    refcounts.remove(name);
                    Long size = pinnedSizes.remove(name);
                    pinnedBytes -= size == null ? 0 : size;
                } else {
                    refcounts.put(name, count);
                }
            }
            openCount--;
            notifyAll();
        }
    }

    synchronized boolean isPinned(String segmentName) {
        return refcounts.containsKey(segmentName);
    }

    /** Names of all currently pinned segments (used by the reaper). */
    synchronized List<String> pinnedSegments() {
        return new ArrayList<>(refcounts.keySet());
    }

    synchronized int openCount() {
        return openCount;
    }

    synchronized long pinnedBytes() {
        return pinnedBytes;
    }

    SnapshotLimits limits() {
        return limits;
    }

    private static long fileSize(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            // segment missing or unreadable: account 0; the snapshot read will
            // surface a distinguishable SNAPSHOT_EXPIRED failure later
            return 0;
        }
    }
}
