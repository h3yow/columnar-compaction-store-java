package com.github.h3yow.columnarcompactionstorejava.store.config;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 存储配置与资源约束：
 * <ul>
 *   <li>{@code maxWriteBatchBytes}：单批次在内存中的估算字节上限，超限批次以
 *       {@code BATCH_TOO_LARGE} 直接拒绝（不进入写流程，避免超限缓冲）；</li>
 *   <li>{@code maxOpenFiles}：全存储同时打开的段文件数上限（信号量），
 *       获取超过 {@code fileAcquireTimeoutMillis} 仍无许可时以 {@code RESOURCE_TIMEOUT} 拒绝；</li>
 *   <li>{@code compactionThreshold}：活动段数达到该值时压实才真正执行，否则返回 NOTHING_TO_COMPACT。</li>
 * </ul>
 */
public record StoreConfig(Path dataDir,
                          long maxWriteBatchBytes,
                          int maxOpenFiles,
                          long fileAcquireTimeoutMillis,
                          int compactionThreshold) {

    /** 默认：单批次 16MB、最多同时打开 8 个文件、等待许可 5 秒、2 个段即可压实。 */
    public static StoreConfig defaults(Path dataDir) {
        Objects.requireNonNull(dataDir, "dataDir");
        return new StoreConfig(dataDir, 16L * 1024 * 1024, 8, 5_000L, 2);
    }

    public StoreConfig {
        Objects.requireNonNull(dataDir, "dataDir");
        if (maxWriteBatchBytes <= 0) {
            throw new IllegalArgumentException("maxWriteBatchBytes 必须为正数");
        }
        // 至少 2：压实需要同时打开 1 个输入段 + 1 个输出段
        if (maxOpenFiles < 2) {
            throw new IllegalArgumentException("maxOpenFiles 至少为 2");
        }
        if (fileAcquireTimeoutMillis < 0) {
            throw new IllegalArgumentException("fileAcquireTimeoutMillis 不能为负");
        }
        if (compactionThreshold < 2) {
            throw new IllegalArgumentException("compactionThreshold 至少为 2");
        }
    }

    public StoreConfig withMaxWriteBatchBytes(long v) {
        return new StoreConfig(dataDir, v, maxOpenFiles, fileAcquireTimeoutMillis, compactionThreshold);
    }

    public StoreConfig withMaxOpenFiles(int v) {
        return new StoreConfig(dataDir, maxWriteBatchBytes, v, fileAcquireTimeoutMillis, compactionThreshold);
    }

    public StoreConfig withFileAcquireTimeoutMillis(long v) {
        return new StoreConfig(dataDir, maxWriteBatchBytes, maxOpenFiles, v, compactionThreshold);
    }

    public StoreConfig withCompactionThreshold(int v) {
        return new StoreConfig(dataDir, maxWriteBatchBytes, maxOpenFiles, fileAcquireTimeoutMillis, v);
    }
}
