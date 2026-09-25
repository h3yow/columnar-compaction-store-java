package com.github.h3yow.columnarcompactionstorejava.store.limiter;

import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 打开文件数上限控制器。
 *
 * <p>每个被打开的段文件（读或写）必须先持有一个许可；许可数上限 =
 * {@code maxOpenFiles}（配置校验要求 &ge; 2，使压实至少能同时打开 1 个输入段与 1 个输出段）。
 * 超限时调用方等待；等待超过 {@code acquireTimeoutMillis} 仍拿不到许可，
 * 以 {@link StoreException.Reason#RESOURCE_TIMEOUT} 被拒绝，拒绝原因与校验类错误可区分。
 * 等待期间线程被中断则以 INTERRUPTED 抛出。
 */
public final class ResourceLimiter {

    /** 一个文件许可；close 幂等，重复释放不计数。 */
    public interface Permit extends AutoCloseable {
        @Override
        void close();
    }

    private final Semaphore semaphore;
    private final int maxOpenFiles;
    private final long acquireTimeoutMillis;

    public ResourceLimiter(int maxOpenFiles, long acquireTimeoutMillis) {
        if (maxOpenFiles < 2) {
            throw new IllegalArgumentException("maxOpenFiles 至少为 2");
        }
        if (acquireTimeoutMillis < 0) {
            throw new IllegalArgumentException("acquireTimeoutMillis 不能为负");
        }
        this.maxOpenFiles = maxOpenFiles;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.semaphore = new Semaphore(maxOpenFiles, true);
    }

    public Permit acquire() {
        boolean acquired;
        try {
            acquired = semaphore.tryAcquire(acquireTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException(StoreException.Reason.INTERRUPTED, "等待文件许可时被中断", e);
        }
        if (!acquired) {
            throw new StoreException(StoreException.Reason.RESOURCE_TIMEOUT,
                    "打开文件数已达上限 " + maxOpenFiles + "，等待 " + acquireTimeoutMillis + "ms 后仍无许可");
        }
        return new Permit() {
            private boolean released;

            @Override
            public synchronized void close() {
                if (!released) {
                    released = true;
                    semaphore.release();
                }
            }
        };
    }

    public int maxOpenFiles() {
        return maxOpenFiles;
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }
}
