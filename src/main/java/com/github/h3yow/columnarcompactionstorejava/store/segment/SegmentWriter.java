package com.github.h3yow.columnarcompactionstorejava.store.segment;

import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.format.SegmentFormat;
import com.github.h3yow.columnarcompactionstorejava.store.model.ColumnDef;
import com.github.h3yow.columnarcompactionstorejava.store.model.DataType;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * 段写入器：逐列写入块，close 时写入尾部索引并 fsync。
 * 必须按 columnId 升序调用 {@link #writeChunk}，每个列最多一次。
 */
public final class SegmentWriter implements Closeable {

    private record IndexItem(int columnId, int rows, int nulls, long offset, long length) {
    }

    private final Path file;
    private final DataOutputStream out;
    private final long segmentId;
    private final long seqStart;
    private final long seqEnd;
    private final int rowCount;
    private final int schemaVersion;
    private final List<IndexItem> indexes = new ArrayList<>();
    private int lastColumnId = -1;
    private boolean closed;

    private SegmentWriter(Path file, long segmentId, long seqStart, long seqEnd,
                          int rowCount, int schemaVersion, DataOutputStream out) {
        this.file = file;
        this.out = out;
        this.segmentId = segmentId;
        this.seqStart = seqStart;
        this.seqEnd = seqEnd;
        this.rowCount = rowCount;
        this.schemaVersion = schemaVersion;
    }

    /** 在 {@code file}（通常是 staging 路径）上创建段写入器，要求文件尚不存在。 */
    public static SegmentWriter create(Path file,
                                       long segmentId,
                                       long seqStart,
                                       long seqEnd,
                                       int rowCount,
                                       int schemaVersion) throws IOException {
        Objects.requireNonNull(file, "file");
        if (seqEnd < seqStart) {
            throw new IllegalArgumentException("seqEnd < seqStart");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount 为负");
        }
        OutputStream raw = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(raw, 64 * 1024));
        writeHeader(dos, segmentId, seqStart, seqEnd, rowCount, schemaVersion);
        return new SegmentWriter(file, segmentId, seqStart, seqEnd, rowCount, schemaVersion, dos);
    }

    private static void writeHeader(DataOutput o, long segmentId, long seqStart, long seqEnd,
                                    int rowCount, int schemaVersion) throws IOException {
        o.writeInt(SegmentFormat.MAGIC);
        o.writeInt(SegmentFormat.VERSION);
        o.writeLong(segmentId);
        o.writeLong(seqStart);
        o.writeLong(seqEnd);
        o.writeInt(rowCount);
        o.writeInt(schemaVersion);
        o.writeInt(0); // chunkCount 占位，close 时无法回填（尾部含全部信息），始终保持 0
    }

    public void writeChunk(ColumnDef column, List<Object> values) throws IOException {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(values, "values");
        if (closed) {
            throw new IllegalStateException("writer 已关闭");
        }
        if (column.id() <= lastColumnId) {
            throw new IllegalArgumentException("列块必须按 columnId 升序且每列一次: " + column.id());
        }
        if (values.size() != rowCount) {
            throw new IllegalArgumentException(
                    "列 " + column.name() + " 块行数 " + values.size() + " 与段行数 " + rowCount + " 不一致");
        }
        lastColumnId = column.id();

        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream payloadOut = new DataOutputStream(payload);
        byte[] nullFlags = new byte[rowCount];
        List<Object> nonNull = new ArrayList<>(rowCount);
        int nulls = 0;
        for (int i = 0; i < rowCount; i++) {
            Object v = values.get(i);
            if (v == null) {
                nullFlags[i] = 1;
                nulls++;
            } else {
                nonNull.add(v);
            }
        }
        payloadOut.write(nullFlags);
        for (Object v : nonNull) {
            encodeValue(payloadOut, column, v);
        }
        payloadOut.flush();
        byte[] payloadBytes = payload.toByteArray();

        CRC32 crc = new CRC32();
        ByteArrayOutputStream frame = new ByteArrayOutputStream(payloadBytes.length + 64);
        DataOutputStream frameOut = new DataOutputStream(frame);
        frameOut.writeInt(column.id());
        frameOut.writeInt(rowCount);
        frameOut.writeInt(nulls);
        frameOut.writeInt(payloadBytes.length);
        frameOut.write(payloadBytes);
        frameOut.flush();
        byte[] frameBytes = frame.toByteArray();
        crc.update(frameBytes);

        long offset = SegmentFormat.HEADER_SIZE + positionSoFar();
        long length = frameBytes.length + 4L;
        synchronized (out) {
            out.write(frameBytes);
            out.writeInt((int) crc.getValue());
        }
        indexes.add(new IndexItem(column.id(), rowCount, nulls, offset, length));
    }

    private long positionSoFar() {
        long p = 0;
        for (IndexItem it : indexes) {
            p += it.length();
        }
        return p;
    }

    private static void encodeValue(DataOutput o, ColumnDef column, Object v) throws IOException {
        DataType t = column.type();
        switch (t) {
            case LONG -> {
                if (!(v instanceof Long)) {
                    throw typeError(column, v);
                }
                o.writeLong((Long) v);
            }
            case DOUBLE -> {
                if (!(v instanceof Double)) {
                    throw typeError(column, v);
                }
                o.writeDouble((Double) v);
            }
            case BOOLEAN -> {
                if (!(v instanceof Boolean)) {
                    throw typeError(column, v);
                }
                o.writeBoolean((Boolean) v);
            }
            case STRING -> {
                if (!(v instanceof String s)) {
                    throw typeError(column, v);
                }
                byte[] utf = s.getBytes(StandardCharsets.UTF_8);
                o.writeInt(utf.length);
                o.write(utf);
            }
            default -> throw new StoreException(StoreException.Reason.BAD_FORMAT, "未知类型: " + t);
        }
    }

    private static StoreException typeError(ColumnDef column, Object v) {
        return new StoreException(StoreException.Reason.BAD_FORMAT,
                "列 " + column.name() + " 期望 " + column.type() + " 实际 " + v.getClass().getName());
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        for (IndexItem it : indexes) {
            out.writeInt(it.columnId());
            out.writeInt(it.rows());
            out.writeInt(it.nulls());
            out.writeLong(it.offset());
            out.writeLong(it.length());
        }
        out.writeInt(indexes.size());
        out.writeInt(SegmentFormat.TRAILER_MAGIC);
        out.flush();
        out.close(); // flush
    }

    public Path file() {
        return file;
    }

    public long segmentId() {
        return segmentId;
    }

    public long seqStart() {
        return seqStart;
    }

    public long seqEnd() {
        return seqEnd;
    }

    public int rowCount() {
        return rowCount;
    }

    public int schemaVersion() {
        return schemaVersion;
    }
}
