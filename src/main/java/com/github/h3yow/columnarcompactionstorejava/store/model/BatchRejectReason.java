package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 批次被拒绝的可区分原因。
 */
public enum BatchRejectReason {
    /** 结构性畸形：空 batchId、空/重复列名、空行集合、null 行等。 */
    MALFORMED_BATCH,
    /** 行缺少列头中声明的字段。 */
    MISSING_FIELD,
    /** 行出现列头未声明的字段。 */
    UNKNOWN_FIELD,
    /** 值的类型与列类型不兼容。 */
    TYPE_MISMATCH,
    /** batchId 已成功写入过，重复提交。 */
    DUPLICATE_BATCH_ID,
    /** 批次估算字节数超过配置的内存上限。 */
    BATCH_TOO_LARGE
}
