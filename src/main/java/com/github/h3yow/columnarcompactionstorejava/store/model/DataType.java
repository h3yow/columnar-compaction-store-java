package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 支持的列数据类型。类型标识会写入段文件，读取端据此解释字节。
 */
public enum DataType {
    LONG,
    DOUBLE,
    STRING,
    BOOLEAN
}
