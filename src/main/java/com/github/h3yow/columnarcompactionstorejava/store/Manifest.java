package com.github.h3yow.columnarcompactionstorejava.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Persistent catalog: current {@link Schema} plus the ordered list of live
 * segment references. This is the single source of truth for snapshot
 * visibility — a reader that has loaded a manifest sees exactly the segments
 * it lists, never a half-compacted state.
 *
 * <p>Each manifest mutation (write, compaction, schema change, retention
 * cleanup) bumps {@link #version()}; an explicitly opened
 * {@link VersionSnapshot} pins one such version.
 *
 * <p>Updates are written to {@code manifest.json.tmp}, fsynced, then
 * atomically renamed over {@code manifest.json}; a crash can therefore leave
 * either the old or the new manifest, never a torn one.
 *
 * <p>Backward compatibility: manifests written before segments carried
 * timestamps (a plain string array, no {@code version} field) load with
 * {@code version = 0} and each segment's creation time set to the load
 * instant, so pre-existing data is never considered expired right after an
 * upgrade.
 */
public final class Manifest {

    /** A live segment: file name plus creation time (millis) used by retention. */
    public record SegmentRef(String name, long createdAtMillis) {
    }

    public static final String FILE_NAME = "manifest.json";
    private static final String TMP_NAME = FILE_NAME + ".tmp";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Schema schema;
    private final List<SegmentRef> segments;
    private final long compactionGeneration;
    private final long version;

    private Manifest(Schema schema, List<SegmentRef> segments,
                     long compactionGeneration, long version) {
        this.schema = schema;
        this.segments = List.copyOf(segments);
        this.compactionGeneration = compactionGeneration;
        this.version = version;
    }

    /** A fresh, empty manifest for a newly created store. */
    public static Manifest empty(Schema schema) {
        return new Manifest(schema, List.of(), 0, 0);
    }

    public Schema schema() {
        return schema;
    }

    /** Names of live segments, in write order. */
    public List<String> liveSegments() {
        return segments.stream().map(SegmentRef::name).toList();
    }

    /** Live segments with their creation timestamps, in write order. */
    public List<SegmentRef> segmentRefs() {
        return segments;
    }

    /** Monotonic counter bumped by every successful compaction. */
    public long compactionGeneration() {
        return compactionGeneration;
    }

    /** Monotonic data version bumped by every manifest mutation. */
    public long version() {
        return version;
    }

    public Manifest withSegments(List<SegmentRef> newSegments, long generation) {
        return new Manifest(schema, newSegments, generation, version + 1);
    }

    public Manifest withSchema(Schema newSchema) {
        return new Manifest(newSchema, segments, compactionGeneration, version + 1);
    }

    /** Loads the manifest from {@code dir}, or {@code null} if none exists yet. */
    public static Manifest load(Path dir) {
        return load(dir, System::currentTimeMillis);
    }

    /**
     * Loads the manifest, stamping legacy timestamp-less segments with
     * {@code clock.getAsLong()}.
     */
    public static Manifest load(Path dir, LongSupplier clock) {
        Path file = dir.resolve(FILE_NAME);
        if (!Files.exists(file)) {
            return null;
        }
        try (InputStream in = Files.newInputStream(file)) {
            ObjectNode root = (ObjectNode) MAPPER.readTree(in);
            List<ColumnDef> columns = new ArrayList<>();
            for (var colNode : root.withArray("columns")) {
                columns.add(new ColumnDef(
                        colNode.get("name").asText(),
                        ColumnType.valueOf(colNode.get("type").asText())));
            }
            int schemaVersion = root.get("schemaVersion").asInt();
            Schema schema = Schema.of(schemaVersion, columns);
            List<SegmentRef> segments = new ArrayList<>();
            for (JsonNode segNode : root.withArray("segments")) {
                if (segNode.isTextual()) {
                    // legacy format: bare file name, no creation time recorded
                    segments.add(new SegmentRef(segNode.asText(), clock.getAsLong()));
                } else {
                    segments.add(new SegmentRef(
                            segNode.get("name").asText(),
                            segNode.get("createdAt").asLong()));
                }
            }
            long generation = root.get("compactionGeneration").asLong();
            JsonNode versionNode = root.get("version");
            long version = versionNode == null ? 0 : versionNode.asLong();
            return new Manifest(schema, segments, generation, version);
        } catch (IOException | RuntimeException e) {
            throw new StoreException(ErrorCode.CORRUPT_SEGMENT,
                    "unreadable manifest " + file + ": " + e.getMessage(), e);
        }
    }

    /** Atomically persists this manifest into {@code dir}. */
    public void save(Path dir) {
        Path tmp = dir.resolve(TMP_NAME);
        Path target = dir.resolve(FILE_NAME);
        ObjectNode root = MAPPER.createObjectNode();
        root.put("schemaVersion", schema.version());
        ArrayNode cols = root.putArray("columns");
        for (ColumnDef col : schema.columns()) {
            ObjectNode c = cols.addObject();
            c.put("name", col.name());
            c.put("type", col.type().name());
        }
        ArrayNode segs = root.putArray("segments");
        for (SegmentRef seg : segments) {
            ObjectNode s = segs.addObject();
            s.put("name", seg.name());
            s.put("createdAt", seg.createdAtMillis());
        }
        root.put("compactionGeneration", compactionGeneration);
        root.put("version", version);
        try (OutputStream out = Files.newOutputStream(tmp)) {
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(out, root);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
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
                    "failed to save manifest: " + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "Manifest{schema=" + schema + ", segments=" + segments
                + ", compactionGeneration=" + compactionGeneration
                + ", version=" + version + "}";
    }
}
