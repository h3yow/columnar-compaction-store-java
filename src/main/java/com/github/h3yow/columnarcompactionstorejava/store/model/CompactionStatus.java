package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 压实触发结果状态。
 */
public enum CompactionStatus {
    /** 实际执行了压实。 */
    COMPACTED,
    /** 活动段数量未达到阈值，无需压实。 */
    NOTHING_TO_COMPACT,
    /** 已有压实正在运行，本次触发未重复执行。 */
    ALREADY_RUNNING
}
