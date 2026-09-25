package com.github.h3yow.columnarcompactionstorejava.store.format;

/**
 * 段文件（{@code seg-NNNNNN.dat}）二进制格式，全部大端编码。
 *
 * <pre>
 * 文件头 (HEADER_SIZE 字节):
 *   int  MAGIC ('C''C''S''G')
 *   int  VERSION                 当前为 1
 *   long segmentId
 *   long seqStart                包含
 *   long seqEnd                  排他
 *   int  rowCount
 *   int  schemaVersion           写入该段时的列数；读取端据此识别旧段缺失的新列
 *   int  chunkCount
 *
 * 若干列块（每个出现的列一个块，按 columnId 升序）:
 *   int  columnId
 *   int  rows                    该块行数（== 段 rowCount）
 *   int  nulls                   null 值个数
 *   int  payloadLength
 *   byte[payloadLength] payload: byte[rows] null 标志位(0/1) + 非 null 值按行序编码
 *                        LONG/DOUBLE 各 8 字节；BOOLEAN 1 字节；
 *                        STRING 为 int utfLength + UTF-8 字节
 *   int  crc32                   对 columnId..payload 全部字节的 CRC32
 *
 * 文件尾 (固定 8 + 28*N 字节，是文件的最后部分):
 *   重复 N 次 { int columnId; int rows; int nulls; long offset; long length }
 *   int  indexEntryCount (== chunkCount)
 *   int  TRAILER_MAGIC ('C''C''T''R')   —— 文件最后 4 字节，用于定位尾部
 * </pre>
 *
 * <p>段文件一经写入不可变；损坏（魔数/CRC 不符）在读取时以 {@code CRC_MISMATCH}/{@code BAD_FORMAT}
 * 明确报错，绝不静默跳过。
 */
public final class SegmentFormat {

    public static final int MAGIC = 0x43435347; // "CCSG"
    public static final int TRAILER_MAGIC = 0x43435452; // "CCTR"
    public static final int VERSION = 1;

    public static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 8 + 4 + 4 + 4;
    public static final int INDEX_ENTRY_SIZE = 4 + 4 + 4 + 8 + 8; // 28
    public static final int TRAILER_FIXED_SIZE = 4 + 4; // indexEntryCount + magic
    public static final int CHUNK_FRAME_SIZE = 4 + 4 + 4 + 4 + 4; // columnId,rows,nulls,payloadLength,crc

    private SegmentFormat() {
    }
}
