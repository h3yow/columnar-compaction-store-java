package com.github.h3yow.columnarcompactionstorejava.store.model;

import java.util.List;

/**
 * 一次快照读取的结果：snapshotSeq 为该快照可见的最大行序号（排他上界），
 * segmentIds 为构成该快照的段，便于解释与校验。
 */
public record ScanResult(Schema schema,
                         List<Row> rows,
                         long snapshotSeq,
                         List<Long> segmentIds,
                         long manifestVersion) {
}
