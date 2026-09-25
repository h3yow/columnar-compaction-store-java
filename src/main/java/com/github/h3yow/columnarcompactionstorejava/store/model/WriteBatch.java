package com.github.h3yow.columnarcompactionstorejava.store.model;

import java.util.List;

/**
 * 一次写入批次：batchId 用于幂等去重；columns 为本批次显式声明的列头；
 * rows 中每一行必须恰好包含 columns 中声明的全部键。
 */
public record WriteBatch(String batchId, List<String> columns, List<Row> rows) {
}
