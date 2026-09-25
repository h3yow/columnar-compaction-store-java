package com.github.h3yow.columnarcompactionstorejava.web;

import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Path;

/**
 * 存储实例持有者：首次使用时懒加载打开本地磁盘存储，进程内单例。
 */
@Service
public class StoreService {

    private final StoreConfig config;
    private volatile DiskColumnarStore store;

    public StoreService(@Value("${store.data-dir:./data}") String dataDir,
                        @Value("${store.max-write-batch-bytes:16777216}") long maxBatchBytes,
                        @Value("${store.max-open-files:8}") int maxOpenFiles,
                        @Value("${store.file-acquire-timeout-millis:5000}") long timeoutMillis,
                        @Value("${store.compaction-threshold:2}") int threshold) {
        this.config = new StoreConfig(Path.of(dataDir), maxBatchBytes, maxOpenFiles,
                timeoutMillis, threshold);
    }

    public synchronized DiskColumnarStore get() {
        if (store == null) {
            store = DiskColumnarStore.open(config);
        }
        return store;
    }
}
