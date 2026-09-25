package com.github.h3yow.columnarcompactionstorejava.store.model;

import java.util.Map;

/**
 * 一行数据：列名 -> 值（值允许为 null，但键必须显式存在）。
 */
public record Row(Map<String, Object> values) {
}
