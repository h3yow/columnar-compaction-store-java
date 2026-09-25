package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.List;
import java.util.Map;

/** A write batch: an ordered list of rows, each row a field-name -> value map. */
public record Batch(List<Map<String, Object>> rows) {
}
