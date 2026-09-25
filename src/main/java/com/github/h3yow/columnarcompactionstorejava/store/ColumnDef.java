package com.github.h3yow.columnarcompactionstorejava.store;

/** Definition of a single column: name + type. */
public record ColumnDef(String name, ColumnType type) {
}
