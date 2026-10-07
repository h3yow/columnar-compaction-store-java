package com.github.h3yow.columnarcompactionstorejava.store;

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

/**
 * Persistent catalog: current {@link Schema} plus the ordered list of live
 * segment file names. This is the single source of truth for snapshot
 * visibility — a reader that has loaded a manifest sees exactly the segments
 * it lists, never a half-compacted state.
 *
 * <p>Updates are written to {@code manifest.json.tmp}, fsynced, then
 * atomically renamed over {@code manifest.json}; a crash can therefore leave
 * either the old or the new manifest, never a torn one.
 */
public final class Manifest {

    public static final String FILE_NAME = "manifest.json";
    private static final String TMP_NAME = FILE_NAME + ".tmp";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Schema schema;
    private final List<String> liveSegments;
    private final long compactionGeneration;
    /** Every version ever made visible, oldest first; trimmed by retention. */
    private final List<VersionInfo> history;

    /** Compatibility constructor used before versions existed. */
    public Manifest(Schema schema, List<String> liveSegments, long compactionGeneration) {
        this(schema, liveSegments, compactionGeneration, List.of());
    }

    public Manifest(Schema schema, List<String> liveSegments, long compactionGeneration,
                    List<VersionInfo> history) {
        this.schema = schema;
        this.liveSegments = List.copyOf(liveSegments);
        this.compactionGeneration = compactionGeneration;
        this.history = List.copyOf(history);
    }

    public Schema schema() {
        return schema;
    }

    public List<String> liveSegments() {
        return liveSegments;
    }

    /** Monotonic counter bumped by every successful compaction. */
    public long compactionGeneration() {
        return compactionGeneration;
    }

    /** Oldest-first catalog of retained historical versions. */
    public List<VersionInfo> history() {
        return history;
    }

    /** Latest visible version, or {@code null} before the first write. */
    public VersionInfo headVersion() {
        return history.isEmpty() ? null : history.get(history.size() - 1);
    }

    /** Looks up a version by id. */
    public VersionInfo version(long versionId) {
        for (VersionInfo v : history) {
            if (v.versionId() == versionId) {
                return v;
            }
        }
        return null;
    }

    public Manifest withSegments(List<String> segments, long generation) {
        return new Manifest(schema, segments, generation, history);
    }

    /** Records a new visible version together with its live-segment set. */
    public Manifest withNewVersion(List<String> segments, long generation,
                                   VersionInfo newVersion) {
        List<VersionInfo> next = new ArrayList<>(history);
        next.add(newVersion);
        return new Manifest(schema, segments, generation, next);
    }

    /** Replaces the version catalog (retention trim / startup reconciliation). */
    public Manifest withHistory(List<VersionInfo> newHistory) {
        return new Manifest(schema, liveSegments, compactionGeneration, newHistory);
    }

    public Manifest withSchema(Schema newSchema) {
        return new Manifest(newSchema, liveSegments, compactionGeneration, history);
    }

    /** Loads the manifest from {@code dir}, or {@code null} if none exists yet. */
    public static Manifest load(Path dir) {
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
            List<String> segments = new ArrayList<>();
            for (var segNode : root.withArray("segments")) {
                segments.add(segNode.asText());
            }
            long generation = root.get("compactionGeneration").asLong();
            List<VersionInfo> history = new ArrayList<>();
            // Absent on stores written before versioned snapshots: old files
            // simply have an empty version catalog.
            if (root.has("versions") && root.get("versions").isArray()) {
                for (var vNode : root.withArray("versions")) {
                    List<String> vSegs = new ArrayList<>();
                    for (var segNode : vNode.withArray("segments")) {
                        vSegs.add(segNode.asText());
                    }
                    history.add(new VersionInfo(
                            vNode.get("versionId").asLong(),
                            vNode.get("createdAtMillis").asLong(),
                            vNode.get("compactionGeneration").asLong(),
                            vNode.path("schemaVersion").asInt(schema.version()),
                            vNode.path("columnCount").asInt(columns.size()),
                            vSegs));
                }
            }
            return new Manifest(schema, segments, generation, history);
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
        for (String seg : liveSegments) {
            segs.add(seg);
        }
        root.put("compactionGeneration", compactionGeneration);
        ArrayNode versions = root.putArray("versions");
        for (VersionInfo v : history) {
            ObjectNode vn = versions.addObject();
            vn.put("versionId", v.versionId());
            vn.put("createdAtMillis", v.createdAtMillis());
            vn.put("compactionGeneration", v.compactionGeneration());
            vn.put("schemaVersion", v.schemaVersion());
            vn.put("columnCount", v.columnCount());
            ArrayNode vSegs = vn.putArray("segments");
            for (String seg : v.segments()) {
                vSegs.add(seg);
            }
        }
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
        return "Manifest{schema=" + schema + ", segments=" + liveSegments
                + ", compactionGeneration=" + compactionGeneration
                + ", versions=" + history.size() + "}";
    }
}
