package com.github.h3yow.columnarcompactionstorejava.store;

import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.Schema;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteResult;

/**
 * 基于本地磁盘的列式存储：批次写入、快照全量读取、压实。
 */
public interface ColumnarStore extends AutoCloseable {

    WriteResult write(WriteBatch batch);

    ScanResult scan();

    CompactionResult compact();

    Schema schema();

    @Override
    void close();
}
