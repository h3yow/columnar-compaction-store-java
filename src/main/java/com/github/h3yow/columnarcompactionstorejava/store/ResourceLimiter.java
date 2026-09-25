package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces explicit upper bounds on two resources:
 * <ul>
 *   <li><b>memory bytes</b> held by in-flight write/compaction buffers</li>
 *   <li><b>open file handles</b> used by segment writers/readers</li>
 * </ul>
 *
 * <p>Acquisition either succeeds, waits up to a timeout, or is rejected with
 * {@link ErrorCode#RESOURCE_LIMIT_EXCEEDED} — never silently proceeds over budget.
 */
public final class ResourceLimiter {

    private final long maxMemoryBytes;
    private final Semaphore filePermits;
    private final AtomicLong memoryInUse = new AtomicLong();

    public ResourceLimiter(long maxMemoryBytes, int maxOpenFiles) {
        if (maxMemoryBytes <= 0 || maxOpenFiles <= 0) {
            throw new IllegalArgumentException("limits must be positive");
        }
        this.maxMemoryBytes = maxMemoryBytes;
        this.filePermits = new Semaphore(maxOpenFiles, true);
    }

    public long maxMemoryBytes() {
        return maxMemoryBytes;
    }

    public long memoryInUse() {
        return memoryInUse.get();
    }

    public int availableFilePermits() {
        return filePermits.availablePermits();
    }

    /**
     * Acquires {@code bytes} of memory budget, waiting up to {@code waitMillis}.
     *
     * @return a handle that must be closed to release the budget
     * @throws StoreException with {@link ErrorCode#RESOURCE_LIMIT_EXCEEDED} if the
     *         budget cannot be satisfied within the wait (or ever, when
     *         {@code bytes} exceeds the total budget)
     */
    public MemoryGrant acquireMemory(long bytes, long waitMillis) {
        if (bytes > maxMemoryBytes) {
            throw new StoreException(ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                    "requested " + bytes + " bytes exceeds total memory budget " + maxMemoryBytes);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        while (true) {
            long current = memoryInUse.get();
            if (current + bytes <= maxMemoryBytes && memoryInUse.compareAndSet(current, current + bytes)) {
                return new MemoryGrant(bytes);
            }
            if (System.nanoTime() >= deadline) {
                throw new StoreException(ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                        "memory budget exhausted: inUse=" + current + " requested=" + bytes
                                + " max=" + maxMemoryBytes);
            }
            try {
                Thread.sleep(1, 0);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException(ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                        "interrupted while waiting for memory budget");
            }
        }
    }

    /**
     * Acquires one open-file permit, waiting up to {@code waitMillis}.
     *
     * @throws StoreException with {@link ErrorCode#RESOURCE_LIMIT_EXCEEDED} on timeout
     */
    public FilePermit acquireFile(long waitMillis) {
        try {
            if (!filePermits.tryAcquire(waitMillis, TimeUnit.MILLISECONDS)) {
                throw new StoreException(ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                        "open-file limit reached, no permit within " + waitMillis + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException(ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                    "interrupted while waiting for file permit");
        }
        return new FilePermit();
    }

    /** Handle for acquired memory budget; close to release. */
    public final class MemoryGrant implements AutoCloseable {
        private final long bytes;
        private boolean released;

        private MemoryGrant(long bytes) {
            this.bytes = bytes;
        }

        public long bytes() {
            return bytes;
        }

        @Override
        public synchronized void close() {
            if (!released) {
                released = true;
                memoryInUse.addAndGet(-bytes);
            }
        }
    }

    /** Handle for one acquired open-file permit; close to release. */
    public final class FilePermit implements AutoCloseable {
        private boolean released;

        private FilePermit() {
        }

        @Override
        public synchronized void close() {
            if (!released) {
                released = true;
                filePermits.release();
            }
        }
    }
}
