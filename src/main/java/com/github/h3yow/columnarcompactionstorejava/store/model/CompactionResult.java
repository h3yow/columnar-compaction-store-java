package com.github.h3yow.columnarcompactionstorejava.store.model;

/**
 * 压实结果。压实前后行数必须相等（rowsBefore == rowsAfter），
 * segmentsBefore &gt;= 2 时被合并为 1 个新段（写入并发时新段会保留在尾部，不被吞掉）。
 */
public record CompactionResult(CompactionStatus status,
                               int segmentsBefore,
                               int segmentsAfter,
                               long rowsBefore,
                               long rowsAfter,
                               Long newSegmentId,
                               long elapsedMillis,
                               String detail) {

    public static CompactionResult compacted(int before, int after, long rows, Long newSegmentId, long elapsedMillis) {
        return new CompactionResult(CompactionStatus.COMPACTED, before, after, rows, rows,
                newSegmentId, elapsedMillis,
                "合并 " + before + " 个段为段 " + newSegmentId + "，逻辑行数 " + rows + " 不变");
    }

    public static CompactionResult skipped(CompactionStatus status, int segments, long rows, String detail) {
        return new CompactionResult(status, segments, segments, rows, rows, null, 0L, detail);
    }
}
