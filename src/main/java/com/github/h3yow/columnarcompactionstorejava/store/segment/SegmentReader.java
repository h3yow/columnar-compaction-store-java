package com.github.h3yow.columnarcompactionstorejava.store.segment;

import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.format.SegmentFormat;
import com.github.h3yow.columnarcompactionstorejava.store.limiter.ResourceLimiter;
import com.github.h3yow.columnarcompactionstorejava.store.model.DataType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * 段读取器：open 时仅读取并校验文件头与尾部索引，列块按需随机读取并做 CRC 校验。
 * 关闭时释放占用的打开文件许可（若有）。
 */
public final class SegmentReader implements Closeable {

    private final Path file;
    private final FileChannel channel;
    private final ResourceLimiter.Permit permit;
    private final long segmentId;
    private final long seqStart;
    private final long seqEnd;
    private final int rowCount;
    private final int schemaVersion;
    private final List<ChunkIndex> chunks;
    private boolean closed;

    private SegmentReader(Path file, FileChannel channel, ResourceLimiter.Permit permit,
                          long segmentId, long seqStart, long seqEnd, int rowCount,
                          int schemaVersion, List<ChunkIndex> chunks) {
        this.file = file;
        this.channel = channel;
        this.permit = permit;
        this.segmentId = segmentId;
        this.seqStart = seqStart;
        this.seqEnd = seqEnd;
        this.rowCount = rowCount;
        this.schemaVersion = schemaVersion;
        this.chunks = chunks;
    }

    /** 打开段并解析头/尾。{@code permit} 可为 null（如离线工具场景）。 */
    public static SegmentReader open(Path file, ResourceLimiter.Permit permit) {
        Objects.requireNonNull(file, "file");
        try {
            FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
            try {
                ByteBuffer hb = readFully(ch, 0L, SegmentFormat.HEADER_SIZE);
                int magic = hb.getInt();
                if (magic != SegmentFormat.MAGIC) {
                    throw new StoreException(StoreException.Reason.BAD_FORMAT,
                            "段文件魔数错误: " + file);
                }
                int version = hb.getInt();
                if (version != SegmentFormat.VERSION) {
                    throw new StoreException(StoreException.Reason.BAD_FORMAT,
                            "不支持的段格式版本 " + version + ": " + file);
                }
                long segId = hb.getLong();
                long seqStart = hb.getLong();
                long seqEnd = hb.getLong();
                int rowCount = hb.getInt();
                int schemaVersion = hb.getInt();
                hb.getInt(); // chunkCount 占位，忽略

                long size = ch.size();
                if (size < SegmentFormat.HEADER_SIZE + SegmentFormat.TRAILER_FIXED_SIZE) {
                    throw new StoreException(StoreException.Reason.BAD_FORMAT,
                            "段文件过短（疑似写入中断残留）: " + file);
                }
                ByteBuffer tail = readFully(ch, size - SegmentFormat.TRAILER_FIXED_SIZE,
                        SegmentFormat.TRAILER_FIXED_SIZE);
                int indexCount = tail.getInt();
                int trailerMagic = tail.getInt();
                if (trailerMagic != SegmentFormat.TRAILER_MAGIC) {
                    // 写入中途崩溃的文件没有完整尾部，明确报损坏而不是被当成数据
                    throw new StoreException(StoreException.Reason.BAD_FORMAT,
                            "段文件尾部魔数错误（疑似未写完/半成品）: " + file);
                }
                long indexBytes = (long) indexCount * SegmentFormat.INDEX_ENTRY_SIZE;
                if (indexCount < 0 || size < SegmentFormat.HEADER_SIZE + indexBytes
                        + SegmentFormat.TRAILER_FIXED_SIZE) {
                    throw new StoreException(StoreException.Reason.BAD_FORMAT,
                            "段文件尾部索引越界: " + file);
                }
                ByteBuffer ib = readFully(ch,
                        size - SegmentFormat.TRAILER_FIXED_SIZE - indexBytes,
                        (int) indexBytes);
                List<ChunkIndex> chunks = new ArrayList<>(indexCount);
                for (int i = 0; i < indexCount; i++) {
                    int columnId = ib.getInt();
                    int rows = ib.getInt();
                    int nulls = ib.getInt();
                    long offset = ib.getLong();
                    long length = ib.getLong();
                    chunks.add(new ChunkIndex(columnId, rows, nulls, offset, length));
                }
                return new SegmentReader(file, ch, permit, segId, seqStart, seqEnd,
                        rowCount, schemaVersion, Collections.unmodifiableList(chunks));
            } catch (RuntimeException e) {
                ch.close();
                throw e;
            }
        } catch (StoreException e) {
            releaseQuietly(permit);
            throw e;
        } catch (IOException e) {
            releaseQuietly(permit);
            throw new StoreException(StoreException.Reason.IO_ERROR, "打开段失败: " + file, e);
        }
    }

    private static void releaseQuietly(ResourceLimiter.Permit permit) {
        if (permit != null) {
            permit.close();
        }
    }

    private static ByteBuffer readFully(FileChannel ch, long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        while (buf.hasRemaining()) {
            int n = ch.read(buf, position + buf.position());
            if (n < 0) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT, "段文件意外结束: " + ch);
            }
        }
        buf.flip();
        return buf;
    }

    public List<Object> readColumn(ChunkIndex chunk, DataType type) {
        Objects.requireNonNull(chunk, "chunk");
        Objects.requireNonNull(type, "type");
        if (closed) {
            throw new IllegalStateException("reader 已关闭");
        }
        if (chunk.length() < SegmentFormat.CHUNK_FRAME_SIZE) {
            throw new StoreException(StoreException.Reason.BAD_FORMAT, "列块长度非法: " + file);
        }
        try {
            int totalLen = (int) chunk.length();
            int frameLen = totalLen - 4;
            ByteBuffer fb = readFully(channel, chunk.offset(), totalLen);
            byte[] frameBytes = fb.array(); // allocate() 生成 heap buffer，array() 一定可用
            int columnId = fb.getInt();
            int rows = fb.getInt();
            int nulls = fb.getInt();
            int payloadLength = fb.getInt();
            if (columnId != chunk.columnId() || rows != chunk.rows() || nulls != chunk.nulls()
                    || payloadLength != frameLen - 16) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "列块元数据与尾部索引不一致: " + file + " 列 " + columnId);
            }
            byte[] payload = new byte[payloadLength];
            fb.get(payload);
            int crcStored = fb.getInt();
            CRC32 crc = new CRC32();
            crc.update(frameBytes, 0, frameLen);
            if ((int) crc.getValue() != crcStored) {
                throw new StoreException(StoreException.Reason.CRC_MISMATCH,
                        "列块 CRC 校验失败（段文件损坏）: " + file + " 列 " + columnId);
            }

            ByteBuffer pb = ByteBuffer.wrap(payload);
            byte[] nullFlags = new byte[rows];
            pb.get(nullFlags);
            List<Object> values = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                if (nullFlags[i] == 1) {
                    values.add(null);
                } else {
                    values.add(decodeValue(pb, type, columnId));
                }
            }
            if (pb.hasRemaining()) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "列块 payload 长度与数据不符: " + file + " 列 " + columnId);
            }
            return values;
        } catch (IOException e) {
            throw new StoreException(StoreException.Reason.IO_ERROR,
                    "读取列块失败: " + file + " 列 " + chunk.columnId(), e);
        }
    }

    private Object decodeValue(ByteBuffer pb, DataType type, int columnId) {
        try {
            return switch (type) {
                case LONG -> pb.getLong();
                case DOUBLE -> pb.getDouble();
                case BOOLEAN -> pb.get() != 0;
                case STRING -> {
                    int len = pb.getInt();
                    if (len < 0) {
                        throw new StoreException(StoreException.Reason.BAD_FORMAT, "字符串长度为负");
                    }
                    byte[] utf = new byte[len];
                    pb.get(utf);
                    yield new String(utf, StandardCharsets.UTF_8);
                }
            };
        } catch (java.nio.BufferUnderflowException e) {
            throw new StoreException(StoreException.Reason.BAD_FORMAT,
                    "列块 payload 数据截断: " + file + " 列 " + columnId, e);
        }
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

    public List<ChunkIndex> chunks() {
        return chunks;
    }

    /** 关闭 channel 并释放文件许可；任何 I/O 失败包装为 StoreException，许可保证释放。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            channel.close();
        } catch (IOException e) {
            throw new StoreException(StoreException.Reason.IO_ERROR,
                    "关闭段文件失败: " + file, e);
        } finally {
            releaseQuietly(permit);
        }
    }
}
