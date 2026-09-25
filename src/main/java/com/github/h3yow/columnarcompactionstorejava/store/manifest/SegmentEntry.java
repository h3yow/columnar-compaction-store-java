package com.github.h3yow.columnarcompactionstorejava.store.manifest;

import java.util.List;

/**
 * 清单中的一个活动段。seqStart 包含、seqEnd 排他；batchIds 用于幂等去重。
 */
public record SegmentEntry(long segmentId,
                           long seqStart,
                           long seqEnd,
                           long rowCount,
                           int schemaVersion,
                           List<String> batchIds) {
}
