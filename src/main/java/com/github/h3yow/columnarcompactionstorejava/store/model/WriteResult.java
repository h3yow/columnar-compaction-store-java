package com.github.h3yow.columnarcompactionstorejava.store.model;

import java.util.Objects;

/**
 * 写入结果。accepted=false 时 rejection 非空；accepted=true 时记录落盘段与序号范围。
 */
public record WriteResult(boolean accepted,
                          String batchId,
                          long segmentId,
                          long firstSeq,
                          long rowCount,
                          BatchRejection rejection) {

    public static WriteResult accepted(String batchId, long segmentId, long firstSeq, long rowCount) {
        Objects.requireNonNull(batchId, "batchId");
        return new WriteResult(true, batchId, segmentId, firstSeq, rowCount, null);
    }

    public static WriteResult rejected(String batchId, BatchRejection rejection) {
        Objects.requireNonNull(rejection, "rejection");
        return new WriteResult(false, batchId, -1L, -1L, 0L, rejection);
    }
}
