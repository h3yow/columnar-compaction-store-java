package com.github.h3yow.columnarcompactionstorejava.store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Binary columnar segment format.
 *
 * <pre>
 *   magic        4 bytes  "CCS1"
 *   formatVer    4 bytes  int, currently 1
 *   schemaVer    4 bytes  int, schema version at write time
 *   rowCount     4 bytes  int
 *   columnCount  4 bytes  int
 *   per column:  nameLen 2 bytes + UTF-8 name, type ordinal 1 byte
 *   data:        column-major; for each column, for each row, the value:
 *                INT 4B | LONG 8B | DOUBLE 8B | BOOL 1B | STRING 4B len + UTF-8
 *   crc32        8 bytes  long, CRC-32 of every preceding byte
 * </pre>
 *
 * <p>Writes go to a {@code .tmp} sibling and are fsynced + atomically renamed,
 * so a crash mid-write can never leave a half-written live segment. Reads
 * verify magic, structural lengths and the CRC; any violation raises
 * {@link ErrorCode#CORRUPT_SEGMENT}.
 */
public final class SegmentFormat {

    static final int MAGIC = 0x43435331; // "CCS1"
    static final int FORMAT_VERSION = 1;
    static final String TMP_SUFFIX = ".tmp";

    private SegmentFormat() {
    }

    /** Rows of a segment, materialized as field-name -> value maps. */
    public record SegmentData(int schemaVersion, List<ColumnDef> columns,
                              List<Map<String, Object>> rows) {
    }

    /**
     * Writes {@code rows} (already validated against {@code schema}) to
     * {@code target} atomically.
     */
    public static void write(Path target, Schema schema, List<Map<String, Object>> rows) {
        Path tmp = target.resolveSibling(target.getFileName() + TMP_SUFFIX);
        CRC32 crc = new CRC32();
        try (OutputStream fos = Files.newOutputStream(tmp);
             OutputStream bos = new BufferedOutputStream(fos);
             CheckedOutputStream cos = new CheckedOutputStream(bos, crc);
             DataOutputStream out = new DataOutputStream(cos)) {
            out.writeInt(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.writeInt(schema.version());
            out.writeInt(rows.size());
            out.writeInt(schema.columnCount());
            for (ColumnDef col : schema.columns()) {
                byte[] name = col.name().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeShort(name.length);
                out.write(name);
                out.writeByte(col.type().ordinal());
            }
            for (ColumnDef col : schema.columns()) {
                for (Map<String, Object> row : rows) {
                    writeValue(out, col.type(), row.get(col.name()));
                }
            }
            out.flush();
            // CRC-32 of every preceding byte, written raw past the checked stream
            long crcValue = crc.getValue();
            bos.write(ByteBuffer.allocate(Long.BYTES).putLong(crcValue).array());
            bos.flush();
            // fsync before rename so the data survives a crash
            try (java.nio.channels.FileChannel ch =
                         java.nio.channels.FileChannel.open(tmp,
                                 java.nio.file.StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // best effort cleanup
            }
            throw new StoreException(ErrorCode.MALFORMED_BATCH,
                    "failed to write segment " + target + ": " + e.getMessage(), e);
        }
    }

    /**
     * Reads and fully validates a segment file.
     *
     * @throws StoreException with {@link ErrorCode#CORRUPT_SEGMENT} on any
     *         magic/length/checksum violation
     */
    public static SegmentData read(Path file) {
        CRC32 crc = new CRC32();
        try (InputStream fis = Files.newInputStream(file);
             InputStream bis = new BufferedInputStream(fis);
             CheckedInputStream cis = new CheckedInputStream(bis, crc);
             DataInputStream in = new DataInputStream(cis)) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw corrupt(file, "bad magic 0x" + Integer.toHexString(magic));
            }
            int formatVersion = in.readInt();
            if (formatVersion != FORMAT_VERSION) {
                throw corrupt(file, "unsupported format version " + formatVersion);
            }
            int schemaVersion = in.readInt();
            int rowCount = in.readInt();
            int columnCount = in.readInt();
            if (rowCount < 0 || columnCount <= 0 || columnCount > 1_000_000) {
                throw corrupt(file, "implausible rowCount=" + rowCount
                        + " columnCount=" + columnCount);
            }
            List<ColumnDef> columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                int nameLen = in.readShort() & 0xFFFF;
                byte[] nameBytes = in.readNBytes(nameLen);
                if (nameBytes.length != nameLen) {
                    throw corrupt(file, "truncated column name");
                }
                int typeOrdinal = in.readByte();
                ColumnType[] types = ColumnType.values();
                if (typeOrdinal < 0 || typeOrdinal >= types.length) {
                    throw corrupt(file, "unknown column type ordinal " + typeOrdinal);
                }
                columns.add(new ColumnDef(
                        new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8),
                        types[typeOrdinal]));
            }
            // column-major payload
            Object[][] values = new Object[columnCount][rowCount];
            for (int c = 0; c < columnCount; c++) {
                for (int r = 0; r < rowCount; r++) {
                    values[c][r] = readValue(in, columns.get(c).type(), file);
                }
            }
            long expectedCrc = readTrailingLong(bis);
            if (expectedCrc != crc.getValue()) {
                throw corrupt(file, "CRC mismatch: expected " + expectedCrc
                        + " computed " + crc.getValue());
            }
            List<Map<String, Object>> rows = new ArrayList<>(rowCount);
            for (int r = 0; r < rowCount; r++) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int c = 0; c < columnCount; c++) {
                    row.put(columns.get(c).name(), values[c][r]);
                }
                rows.add(row);
            }
            return new SegmentData(schemaVersion, columns, rows);
        } catch (NoSuchFileException e) {
            throw new StoreException(ErrorCode.SNAPSHOT_DATA_UNAVAILABLE,
                    "segment file disappeared " + file.getFileName() + ": " + e.getMessage(), e);
        } catch (EOFException e) {
            throw corrupt(file, "unexpected end of file", e);
        } catch (IOException e) {
            throw corrupt(file, "I/O error: " + e.getMessage(), e);
        }
    }

    private static long readTrailingLong(InputStream in) throws IOException {
        byte[] buf = in.readNBytes(Long.BYTES);
        if (buf.length != Long.BYTES) {
            throw new EOFException("missing CRC trailer");
        }
        if (in.read() != -1) {
            throw new IOException("trailing bytes after CRC");
        }
        return ByteBuffer.wrap(buf).getLong();
    }

    private static void writeValue(DataOutputStream out, ColumnType type, Object value)
            throws IOException {
        switch (type) {
            case INT -> out.writeInt((Integer) value);
            case LONG -> out.writeLong((Long) value);
            case DOUBLE -> out.writeDouble((Double) value);
            case BOOL -> out.writeBoolean((Boolean) value);
            case STRING -> {
                byte[] bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
        }
    }

    private static Object readValue(DataInputStream in, ColumnType type, Path file)
            throws IOException {
        return switch (type) {
            case INT -> in.readInt();
            case LONG -> in.readLong();
            case DOUBLE -> in.readDouble();
            case BOOL -> in.readBoolean();
            case STRING -> {
                int len = in.readInt();
                if (len < 0 || len > 64 * 1024 * 1024) {
                    throw corrupt(file, "implausible string length " + len);
                }
                byte[] bytes = in.readNBytes(len);
                if (bytes.length != len) {
                    throw new EOFException("truncated string value");
                }
                yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            }
        };
    }

    private static StoreException corrupt(Path file, String why) {
        return new StoreException(ErrorCode.CORRUPT_SEGMENT,
                "corrupt segment " + file.getFileName() + ": " + why);
    }

    private static StoreException corrupt(Path file, String why, Throwable cause) {
        return new StoreException(ErrorCode.CORRUPT_SEGMENT,
                "corrupt segment " + file.getFileName() + ": " + why, cause);
    }
}
