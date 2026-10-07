package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory pin table for open {@link VersionSnapshot}s.
 *
 * <ul>
 *   <li>Every backing segment is reference-counted across open snapshots;
 *       shared segments are counted once in the pinned-byte union.</li>
 *   <li>{@link SnapshotLimits} are enforced on attach: over-limit callers
 *       wait for capacity (released snapshots signal the waiters) and are
 *       then rejected with a distinguishable {@link ErrorCode} —
 *       {@link ErrorCode#SNAPSHOT_LIMIT_EXCEEDED} or
 *       {@link ErrorCode#SNAPSHOT_BYTES_LIMIT_EXCEEDED}.</li>
 *   <li>Release is idempotent and returns all capacity: refcounts, the byte
 *       union and the snapshot count move exactly once per open/close, so
 *       interleaving with cleanup/compaction never double counts or leaks.</li>
 * </ul>
 */
final class SnapshotRegistry {

    private record PinEntry(VersionSnapshot snapshot, long addedBytes) {
    }

    private final SnapshotLimits limits;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition capacityChanged = lock.newCondition();

    private final Map<Long, PinEntry> bySnapshotId = new HashMap<>();
    private final Map<String, Integer> segmentRefs = new HashMap<>();
    private final Map<String, Long> segmentBytes = new HashMap<>();
    private long pinnedUnionBytes;

    SnapshotRegistry(SnapshotLimits limits) {
        this.limits = limits;
    }

    SnapshotLimits limits() {
        return limits;
    }

    /**
     * Pins {@code snapshot}; {@code segmentSizes} lists the on-disk size of
     * every segment backing its version. Waits up to {@code waitMillis} for
     * count/byte capacity. On failure nothing is pinned.
     */
    void attach(VersionSnapshot snapshot, Map<String, Long> segmentSizes, long waitMillis) {
        long totalVersionBytes = 0;
        for (long size : segmentSizes.values()) {
            totalVersionBytes += size;
        }
        // A version larger than the whole budget can never be served.
        if (totalVersionBytes > limits.maxPinnedBytes()) {
            throw new StoreException(ErrorCode.SNAPSHOT_BYTES_LIMIT_EXCEEDED,
                    "snapshot of v" + snapshot.versionId() + " needs " + totalVersionBytes
                            + " pinned bytes, exceeding total budget "
                            + limits.maxPinnedBytes());
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        lock.lock();
        try {
            while (true) {
                long addedBytes = 0;
                for (Map.Entry<String, Long> e : segmentSizes.entrySet()) {
                    if (segmentRefs.getOrDefault(e.getKey(), 0) == 0) {
                        addedBytes += e.getValue();
                    }
                }
                boolean countFull = bySnapshotId.size() + 1 > limits.maxOpenSnapshots();
                boolean bytesFull = pinnedUnionBytes + addedBytes > limits.maxPinnedBytes();
                if (!countFull && !bytesFull) {
                    for (Map.Entry<String, Long> e : segmentSizes.entrySet()) {
                        String name = e.getKey();
                        int refs = segmentRefs.merge(name, 1, Integer::sum);
                        if (refs == 1) {
                            segmentBytes.put(name, e.getValue());
                            pinnedUnionBytes += e.getValue();
                        }
                    }
                    bySnapshotId.put(snapshot.snapshotId(),
                            new PinEntry(snapshot, addedBytes));
                    return;
                }
                if (System.nanoTime() >= deadline) {
                    if (countFull) {
                        throw new StoreException(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED,
                                "open-snapshot limit " + limits.maxOpenSnapshots()
                                        + " reached; no capacity within " + waitMillis + "ms");
                    }
                    throw new StoreException(ErrorCode.SNAPSHOT_BYTES_LIMIT_EXCEEDED,
                            "pinned-byte budget " + limits.maxPinnedBytes()
                                    + " exhausted (would add " + addedBytes + " of "
                                    + pinnedUnionBytes + " in use); no capacity within "
                                    + waitMillis + "ms");
                }
                try {
                    capacityChanged.awaitNanos(deadline - System.nanoTime());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new StoreException(ErrorCode.SNAPSHOT_LIMIT_EXCEEDED,
                            "interrupted while waiting for snapshot capacity");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Idempotently unpins everything held by {@code snapshot}. */
    void release(VersionSnapshot snapshot) {
        lock.lock();
        try {
            PinEntry entry = bySnapshotId.remove(snapshot.snapshotId());
            if (entry == null) {
                return; // idempotent: double release / rejected attach
            }
            for (String name : snapshot.segments()) {
                int refs = segmentRefs.get(name) - 1;
                if (refs == 0) {
                    segmentRefs.remove(name);
                    Long size = segmentBytes.remove(name);
                    if (size != null) {
                        pinnedUnionBytes -= size;
                    }
                } else {
                    segmentRefs.put(name, refs);
                }
            }
            capacityChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    Set<String> pinnedSegments() {
        lock.lock();
        try {
            return new LinkedHashSet<>(segmentRefs.keySet());
        } finally {
            lock.unlock();
        }
    }

    boolean isPinned(String segmentName) {
        lock.lock();
        try {
            return segmentRefs.getOrDefault(segmentName, 0) > 0;
        } finally {
            lock.unlock();
        }
    }

    int activeCount() {
        lock.lock();
        try {
            return bySnapshotId.size();
        } finally {
            lock.unlock();
        }
    }

    long pinnedUnionBytes() {
        lock.lock();
        try {
            return pinnedUnionBytes;
        } finally {
            lock.unlock();
        }
    }

    Set<Long> pinnedVersionIds() {
        lock.lock();
        try {
            Set<Long> ids = new LinkedHashSet<>();
            for (PinEntry entry : bySnapshotId.values()) {
                ids.add(entry.snapshot().versionId());
            }
            return ids;
        } finally {
            lock.unlock();
        }
    }

    List<VersionSnapshot> activeSnapshots() {
        lock.lock();
        try {
            return bySnapshotId.values().stream().map(PinEntry::snapshot).toList();
        } finally {
            lock.unlock();
        }
    }
}
