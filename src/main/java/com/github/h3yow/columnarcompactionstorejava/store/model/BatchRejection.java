package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 批次拒绝结果：原因 + 关联列 + 人类可解释的细节。
 */
public record BatchRejection(BatchRejectReason reason, String column, String detail) {
}
