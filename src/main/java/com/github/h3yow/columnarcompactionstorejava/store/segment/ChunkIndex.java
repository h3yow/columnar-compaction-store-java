package com.github.h3yow.columnarcompactionstorejava.store.segment;

/**
 * 段尾索引中的一个列块定位信息。
 */
public record ChunkIndex(int columnId, int rows, int nulls, long offset, long length) {
}
