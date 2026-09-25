package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 列定义：列 id 在 schema 演进中只增不变，是列式存储与读取对齐的稳定依据。
 */
public record ColumnDef(int id, String name, DataType type) {
}
