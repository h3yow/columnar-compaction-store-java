package com.github.h3yow.columnarcompactionstorejava.store.manifest;

import com.github.h3yow.columnarcompactionstorejava.store.model.ColumnDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 清单：当前 schema、活动段列表（顺序即逻辑读取顺序）、段/行序号计数器。
 *
 * <p>清单的一次原子替换就是一次快照边界：读取方持有一个 Manifest 引用期间，
 * 看到的段集合永不变更；清单中列出的段文件视为不可变，绝不会被原地修改。
 */
public final class Manifest {

    private final List<ColumnDef> columns;
    private final List<SegmentEntry> segments;
    private final long nextSegmentId;
    private final long nextSeq;
    private final long version;

    public Manifest(List<ColumnDef> columns,
                    List<SegmentEntry> segments,
                    long nextSegmentId,
                    long nextSeq,
                    long version) {
        this.columns = List.copyOf(Objects.requireNonNull(columns));
        this.segments = List.copyOf(Objects.requireNonNull(segments));
        if (nextSegmentId < 0 || nextSeq < 0 || version < 0) {
            throw new IllegalArgumentException("计数器为负");
        }
        this.nextSegmentId = nextSegmentId;
        this.nextSeq = nextSeq;
        this.version = version;
    }

    /** 初始空清单（数据目录尚未初始化时使用）。 */
    public static Manifest initial() {
        return new Manifest(List.of(), List.of(), 1L, 0L, 0L);
    }

    /** 产生替换后的新清单（schema 与段列表同时切换），版本号 +1。 */
    public Manifest replaced(List<ColumnDef> newColumns,
                             List<SegmentEntry> newSegments,
                             long newNextSegmentId,
                             long newNextSeq) {
        return new Manifest(newColumns, newSegments, newNextSegmentId, newNextSeq, version + 1);
    }

    public List<ColumnDef> columns() {
        return columns;
    }

    public List<SegmentEntry> segments() {
        return segments;
    }

    public long nextSegmentId() {
        return nextSegmentId;
    }

    public long nextSeq() {
        return nextSeq;
    }

    public long version() {
        return version;
    }

    public long totalRows() {
        long sum = 0;
        for (SegmentEntry e : segments) {
            sum += e.rowCount();
        }
        return sum;
    }

    /** 已成功提交过的 batchId（仅在内存全量扫描代价可接受时使用；段内不重复）。 */
    public List<String> allBatchIds() {
        List<String> all = new ArrayList<>();
        for (SegmentEntry e : segments) {
            all.addAll(e.batchIds());
        }
        return Collections.unmodifiableList(all);
    }
}
