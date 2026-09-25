package com.github.h3yow.columnarcompactionstorejava.store.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.model.ColumnDef;
import com.github.h3yow.columnarcompactionstorejava.store.model.DataType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 清单持久化。
 *
 * <p>写协议（崩溃安全）：写 {@code manifest.json.tmp} → fsync 文件 →
 * 原子 rename 覆盖 {@code manifest.json} → fsync 目录。任何时刻旧清单要么完整存在、
 * 要么完整替换，不会出现读到半个清单的情况。
 *
 * <p>启动恢复（{@link #recoverOrRead}）：
 * <ul>
 *   <li>删除所有 {@code .dat.tmp}：这些只可能是写入/压实 rename 前崩溃留下的半成品；</li>
 *   <li>{@code manifest.json.tmp} 直接删除（rename 未发生，正式清单仍有效）；</li>
 *   <li>清单存在但无法解析时拒绝启动（BAD_FORMAT），绝不静默丢弃元数据；</li>
 *   <li>清单缺失（首次初始化或极端情况）：返回 null，由存储层初始化为空；
 *       此时若存在 .dat 孤儿文件，会被移入 {@code orphan/} 并打警告，不直接删除；</li>
 *   <li>清单正常但存在未引用的 {@code .dat}：说明对应替换已完成、文件未及删除，
 *       移入 {@code orphan/} 以便审计，不作为数据读取。</li>
 * </ul>
 */
public final class ManifestStore {

    private static final String MANIFEST_NAME = "manifest.json";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path dataDir;
    private final ObjectMapper mapper = new ObjectMapper();

    public ManifestStore(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void write(Manifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        try {
            Files.createDirectories(dataDir);
            Path target = manifestPath();
            Path tmp = dataDir.resolve(MANIFEST_NAME + TMP_SUFFIX);
            try (OutputStream os = Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(os, toJson(manifest));
                os.flush();
                if (os instanceof java.io.FileOutputStream fos) {
                    fos.getFD().sync();
                }
            }
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            fsyncDir(dataDir);
        } catch (IOException e) {
            throw new StoreException(StoreException.Reason.IO_ERROR, "持久化清单失败", e);
        }
    }

    /**
     * 启动恢复 + 读取。返回 null 表示目录尚无清单（首次初始化）。
     */
    public Manifest recoverOrRead() {
        try {
            Files.createDirectories(dataDir);
            // 1. 清理所有 rename 前的半成品
            deleteFilesBySuffix(dataDir, TMP_SUFFIX);

            Path mf = manifestPath();
            if (!Files.exists(mf)) {
                quarantineOrphans(Set.of());
                return null;
            }
            Manifest manifest;
            try {
                manifest = fromJson(mapper.readTree(Files.readAllBytes(mf)));
            } catch (IOException | RuntimeException e) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "清单损坏或格式无法解析，拒绝启动以避免数据丢失: " + mf, e);
            }
            Set<Long> referenced = new HashSet<>();
            for (SegmentEntry e : manifest.segments()) {
                referenced.add(e.segmentId());
            }
            quarantineOrphans(referenced);
            return manifest;
        } catch (IOException e) {
            throw new StoreException(StoreException.Reason.IO_ERROR, "读取清单/恢复目录失败: " + dataDir, e);
        }
    }

    private void deleteFilesBySuffix(Path dir, String suffix) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*" + suffix)) {
            for (Path p : ds) {
                Files.deleteIfExists(p);
            }
        }
    }

    private void quarantineOrphans(Set<Long> referencedSegmentIds) throws IOException {
        Path orphanDir = dataDir.resolve("orphan");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir, "seg-*.dat")) {
            for (Path p : ds) {
                Long id = parseSegmentId(p.getFileName().toString());
                if (id == null || !referencedSegmentIds.contains(id)) {
                    Files.createDirectories(orphanDir);
                    Files.move(p, orphanDir.resolve(p.getFileName().toString()),
                            StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private Long parseSegmentId(String name) {
        // seg-NNNNNN.dat
        if (!name.startsWith("seg-") || !name.endsWith(".dat")) {
            return null;
        }
        try {
            return Long.parseLong(name.substring(4, name.length() - 4));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Path segmentFile(Path dataDir, long segmentId) {
        return dataDir.resolve(String.format("seg-%06d.dat", segmentId));
    }

    public static Path stagingSegmentFile(Path dataDir, long segmentId) {
        return dataDir.resolve(String.format("seg-%06d.dat.tmp", segmentId));
    }

    private static void fsyncDir(Path dir) throws IOException {
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(dir,
                StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignored) {
            // 部分平台不支持目录 fsync，rename 原子性已足够保证一致
        }
    }

    public Path manifestPath() {
        return dataDir.resolve(MANIFEST_NAME);
    }

    // ---------- JSON ----------

    private Map<String, Object> toJson(Manifest m) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("formatVersion", 1);
        root.put("nextSegmentId", m.nextSegmentId());
        root.put("nextSeq", m.nextSeq());
        root.put("version", m.version());
        List<Map<String, Object>> cols = new ArrayList<>();
        for (ColumnDef c : m.columns()) {
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("id", c.id());
            cm.put("name", c.name());
            cm.put("type", c.type().name());
            cols.add(cm);
        }
        root.put("columns", cols);
        List<Map<String, Object>> segs = new ArrayList<>();
        for (SegmentEntry e : m.segments()) {
            Map<String, Object> em = new LinkedHashMap<>();
            em.put("segmentId", e.segmentId());
            em.put("seqStart", e.seqStart());
            em.put("seqEnd", e.seqEnd());
            em.put("rowCount", e.rowCount());
            em.put("schemaVersion", e.schemaVersion());
            em.put("batchIds", e.batchIds());
            segs.add(em);
        }
        root.put("segments", segs);
        return root;
    }

    private Manifest fromJson(com.fasterxml.jackson.databind.JsonNode root) {
        int formatVersion = root.path("formatVersion").asInt(1);
        if (formatVersion != 1) {
            throw new StoreException(StoreException.Reason.BAD_FORMAT,
                    "不支持的清单格式版本: " + formatVersion);
        }
        List<ColumnDef> columns = new ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode cn : root.get("columns")) {
            int id = cn.get("id").asInt();
            String name = cn.get("name").asText();
            DataType type = DataType.valueOf(cn.get("type").asText());
            columns.add(new ColumnDef(id, name, type));
        }
        List<SegmentEntry> segments = new ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode en : root.get("segments")) {
            List<String> batchIds = new ArrayList<>();
            en.get("batchIds").forEach(b -> batchIds.add(b.asText()));
            segments.add(new SegmentEntry(
                    en.get("segmentId").asLong(),
                    en.get("seqStart").asLong(),
                    en.get("seqEnd").asLong(),
                    en.get("rowCount").asLong(),
                    en.get("schemaVersion").asInt(),
                    List.copyOf(batchIds)));
        }
        return new Manifest(columns, segments,
                root.get("nextSegmentId").asLong(),
                root.get("nextSeq").asLong(),
                root.get("version").asLong());
    }
}

