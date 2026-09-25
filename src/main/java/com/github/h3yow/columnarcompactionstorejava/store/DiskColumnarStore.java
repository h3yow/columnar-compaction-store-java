package com.github.h3yow.columnarcompactionstorejava.store;

import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.limiter.ResourceLimiter;
import com.github.h3yow.columnarcompactionstorejava.store.manifest.Manifest;
import com.github.h3yow.columnarcompactionstorejava.store.manifest.ManifestStore;
import com.github.h3yow.columnarcompactionstorejava.store.manifest.SegmentEntry;
import com.github.h3yow.columnarcompactionstorejava.store.model.ColumnDef;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionStatus;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.Schema;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteResult;
import com.github.h3yow.columnarcompactionstorejava.store.segment.ChunkIndex;
import com.github.h3yow.columnarcompactionstorejava.store.segment.SegmentReader;
import com.github.h3yow.columnarcompactionstorejava.store.segment.SegmentWriter;
import com.github.h3yow.columnarcompactionstorejava.store.validation.BatchValidator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 磁盘列式存储实现。
 *
 * <h3>快照与并发模型</h3>
 * <ul>
 *   <li>{@link Manifest} 为不可变对象，{@code volatile} 发布；每次提交用
 *       「临时文件 + 原子 rename + fsync」整体替换，是唯一的快照边界；</li>
 *   <li>段文件不可变。读取方在短暂的读锁内快照清单并为段登记引用计数，随后无锁进行 I/O；
 *     压实/写入的提交在写锁内完成，因此读方拿到的段集合要么是旧快照、要么是新快照，
 *     绝不会看到半成品状态；</li>
 *   <li>压实替换出的旧段先标记待删，等所有在途读取方引用归零后才物理删除，
 *     删除失败（如平台占用）则移入 {@code orphan/}，不影响正确性；</li>
 *   <li>{@code compacting} 标志保证重复/并发触发压实直接返回 ALREADY_RUNNING，
 *     不会产生重复段或重复计数。</li>
 * </ul>
 *
 * <h3>写入事务</h3>
 * 校验（含内存上限、batchId 去重快照）→ 分配段/行序号 → 写 staging 段并 fsync →
 * 写锁内再次去重 → rename → 写清单并 fsync → 发布。任何提交前失败只删除 staging，
 * 已发布数据不受影响；批次被拒绝时不产生任何文件、不改 schema、不消耗序号。
 */
public final class DiskColumnarStore implements ColumnarStore {

    private static final Logger log = LoggerFactory.getLogger(DiskColumnarStore.class);

    private final StoreConfig config;
    private final ManifestStore manifestStore;
    private final ResourceLimiter fileLimiter;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final AtomicBoolean compacting = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 段引用计数：仅当前/旧清单中的段会被读取方登记。 */
    private final ConcurrentHashMap<Long, AtomicInteger> segmentRefs = new ConcurrentHashMap<>();
    /** 已被清单替换、等待引用归零后删除的段。 */
    private final Set<Long> pendingDelete = new HashSet<>();
    private final Object refsLock = new Object();

    private final AtomicLong segmentIdCounter;

    private volatile Manifest manifest;
    private volatile Schema currentSchema;

    /** 测试用：压实 staging 完成、提交之前触发；抛异常可模拟中途失败。 */
    private volatile Runnable compactionFailureHook = null;

    private DiskColumnarStore(StoreConfig config, Manifest manifest) {
        this.config = config;
        this.manifestStore = new ManifestStore(config.dataDir());
        this.fileLimiter = new ResourceLimiter(config.maxOpenFiles(), config.fileAcquireTimeoutMillis());
        this.manifest = manifest;
        this.currentSchema = new Schema(manifest.columns());
        this.segmentIdCounter = new AtomicLong(manifest.nextSegmentId());
    }

    /** 打开（必要时初始化）数据目录。残留 staging 在清单层恢复时已清理。 */
    public static DiskColumnarStore open(StoreConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config 为 null");
        }
        try {
            Files.createDirectories(config.dataDir());
        } catch (IOException e) {
            throw new StoreException(StoreException.Reason.IO_ERROR, "创建数据目录失败: " + config.dataDir(), e);
        }
        ManifestStore ms = new ManifestStore(config.dataDir());
        Manifest m = ms.recoverOrRead();
        if (m == null) {
            m = Manifest.initial();
            ms.write(m);
        }
        validateManifestIntegrity(config, m);
        return new DiskColumnarStore(config, m);
    }

    private static void validateManifestIntegrity(StoreConfig config, Manifest m) {
        long expectedSeq = 0L;
        Set<Long> ids = new HashSet<>();
        List<ColumnDef> cols = m.columns();
        for (SegmentEntry e : m.segments()) {
            if (!ids.add(e.segmentId())) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "清单中段 id 重复: " + e.segmentId());
            }
            if (e.seqStart() != expectedSeq) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "清单段序号不连续: 段 " + e.segmentId()
                                + " 起始 " + e.seqStart() + " 期望 " + expectedSeq);
            }
            if (e.seqEnd() <= e.seqStart() || e.rowCount() != e.seqEnd() - e.seqStart()) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "清单段序号范围非法: 段 " + e.segmentId());
            }
            if (e.schemaVersion() > cols.size()) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "段 " + e.segmentId() + " 的 schemaVersion 大于当前列数");
            }
            Path f = ManifestStore.segmentFile(config.dataDir(), e.segmentId());
            if (!Files.exists(f)) {
                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                        "清单引用的段文件缺失: " + f);
            }
            expectedSeq = e.seqEnd();
        }
        if (expectedSeq != m.nextSeq()) {
            throw new StoreException(StoreException.Reason.BAD_FORMAT,
                    "清单 nextSeq=" + m.nextSeq() + " 与段范围合计 " + expectedSeq + " 不符");
        }
    }

    @Override
    public WriteResult write(WriteBatch batch) {
        checkOpen();
        String batchId = batch == null ? null : batch.batchId();

        // 写事务整体串行（写锁）：保证序号区间从清单权威值连续分配、
        // batchId 去重无竞态、新列类型裁决无竞态。事务中任何失败都不消耗 seq、不发布文件。
        // 读方的快照获取只需读锁，仅在提交瞬间短暂等待；耗时的压实 I/O 不持写锁，不受影响。
        rwLock.writeLock().lock();
        Path staging = null;
        boolean committed = false;
        try {
            Manifest current = this.manifest;
            Set<String> known = new HashSet<>(current.allBatchIds());
            Object validated = BatchValidator.validate(batch, currentSchema, known,
                    config.maxWriteBatchBytes());
            if (validated instanceof com.github.h3yow.columnarcompactionstorejava.store.model.BatchRejection rej) {
                log.warn("写入被拒绝 batchId={} 原因={} 列={} 依据={}",
                        batchId, rej.reason(), rej.column(), rej.detail());
                return WriteResult.rejected(batchId, rej);
            }
            BatchValidator.Resolved resolved = (BatchValidator.Resolved) validated;

            long segId = segmentIdCounter.getAndIncrement();
            int rowCount = batch.rows().size();
            long seqStart = current.nextSeq(); // 权威、连续
            long seqEnd = seqStart + rowCount;
            int newSchemaVersion = currentSchema.version() + resolved.newColumns().size();
            staging = ManifestStore.stagingSegmentFile(config.dataDir(), segId);

            ResourceLimiter.Permit permit = fileLimiter.acquire();
            SegmentWriter writer;
            try {
                writer = SegmentWriter.create(staging, segId, seqStart, seqEnd,
                        rowCount, newSchemaVersion);
                try {
                    for (int i = 0; i < resolved.columns().size(); i++) {
                        writer.writeChunk(resolved.columns().get(i), resolved.columnValues().get(i));
                    }
                } finally {
                    writer.close();
                }
            } finally {
                permit.close();
            }

            Path target = ManifestStore.segmentFile(config.dataDir(), segId);
            try {
                Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(staging, target);
            }

            List<ColumnDef> newColumns = new ArrayList<>(current.columns());
            newColumns.addAll(resolved.newColumns());
            List<SegmentEntry> newEntries = new ArrayList<>(current.segments());
            newEntries.add(new SegmentEntry(segId, seqStart, seqEnd, rowCount,
                    newColumns.size(), List.of(batchId)));
            newEntries.sort((a, b) -> Long.compare(a.seqStart(), b.seqStart()));

            Manifest published = current.replaced(newColumns, newEntries,
                    segmentIdCounter.get(), seqEnd);
            manifestStore.write(published);
            this.manifest = published;
            this.currentSchema = new Schema(published.columns());
            committed = true;
            log.info("写入接受 batchId={} 段={} seq=[{},{}) 行数={} 新列数={}",
                    batchId, segId, seqStart, seqEnd, rowCount, resolved.newColumns().size());
            return WriteResult.accepted(batchId, segId, seqStart, rowCount);
        } catch (StoreException e) {
            deleteQuietly(staging);
            throw e;
        } catch (Exception e) {
            deleteQuietly(staging);
            throw new StoreException(StoreException.Reason.IO_ERROR,
                    "写入段失败 batchId=" + batchId, e);
        } finally {
            rwLock.writeLock().unlock();
            if (!committed) {
                deleteQuietly(staging);
            }
        }
    }

    @Override
    public ScanResult scan() {
        checkOpen();
        Manifest snapshot;
        List<SegmentEntry> entries;
        List<ResourceLimiter.Permit> permits = new ArrayList<>();
        rwLock.readLock().lock();
        try {
            snapshot = this.manifest;
            entries = snapshot.segments();
            acquireRefs(entries);
        } finally {
            rwLock.readLock().unlock();
        }
        try {
            Schema schema = new Schema(snapshot.columns());
            List<Row> allRows = new ArrayList<>((int) Math.min(Integer.MAX_VALUE, snapshot.totalRows()));
            for (SegmentEntry entry : entries) {
                Path file = ManifestStore.segmentFile(config.dataDir(), entry.segmentId());
                // open 失败时许可已在 reader 内部释放；成功后才登记，避免双重释放
                ResourceLimiter.Permit permit = fileLimiter.acquire();
                SegmentReader reader = SegmentReader.open(file, permit);
                permits.add(permit);
                try {
                    List<List<Object>> table = new ArrayList<>(schema.columns().size());
                    Map<Integer, ChunkIndex> chunkByCol = new LinkedHashMap<>();
                    for (ChunkIndex ci : reader.chunks()) {
                        chunkByCol.put(ci.columnId(), ci);
                    }
                    for (ColumnDef col : schema.columns()) {
                        ChunkIndex ci = chunkByCol.get(col.id());
                        List<Object> values;
                        // 旧段（写入时列数更少）或块缺失：新列对旧行的缺省语义恒为 null
                        if (ci == null || col.id() >= entry.schemaVersion()) {
                            values = Collections.nCopies((int) entry.rowCount(), null);
                        } else {
                            if (ci.rows() != entry.rowCount()) {
                                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                                        "段 " + entry.segmentId() + " 列 " + col.id()
                                                + " 块行数与清单不符");
                            }
                            values = reader.readColumn(ci, col.type());
                        }
                        table.add(values);
                    }
                    for (int r = 0; r < entry.rowCount(); r++) {
                        Map<String, Object> rowMap = new LinkedHashMap<>(schema.columns().size() * 2);
                        for (int c = 0; c < schema.columns().size(); c++) {
                            rowMap.put(schema.columns().get(c).name(), table.get(c).get(r));
                        }
                        allRows.add(new Row(rowMap));
                    }
                } finally {
                    reader.close(); // 内部已释放其许可
                    permits.remove(permit);
                }
            }
            List<Long> ids = new ArrayList<>(entries.size());
            for (SegmentEntry e : entries) {
                ids.add(e.segmentId());
            }
            log.info("快照读取 清单版本={} 段数={} 行数={} snapshotSeq={}",
                    snapshot.version(), entries.size(), allRows.size(), snapshot.nextSeq());
            return new ScanResult(schema, allRows, snapshot.nextSeq(),
                    Collections.unmodifiableList(ids), snapshot.version());
        } finally {
            releaseRefs(entries);
            for (ResourceLimiter.Permit p : permits) {
                p.close(); // 防御性：reader 打开失败等异常路径
            }
        }
    }

    @Override
    public CompactionResult compact() {
        checkOpen();
        long startNanos = System.nanoTime();
        if (!compacting.compareAndSet(false, true)) {
            Manifest m = this.manifest;
            log.info("压实触发被跳过：已有压实运行中 段数={} 行数={}", m.segments().size(), m.totalRows());
            return CompactionResult.skipped(CompactionStatus.ALREADY_RUNNING,
                    m.segments().size(), m.totalRows(),
                    "已有压实正在运行，重复触发被忽略，不产生重复段");
        }
        Path staging = null;
        boolean committed = false;
        try {
            Manifest plan;
            List<SegmentEntry> inputs;
            rwLock.readLock().lock();
            try {
                plan = this.manifest;
                inputs = new ArrayList<>(plan.segments());
                // 注意：压实不给输入段登记读取引用。压实是段文件唯一的删除者，且由
                // compacting 标志保证全局单实例；若在此自引用，提交后等待归零时会自我死锁。
                // 真正需要保护的在途读者由 scan() 登记引用，提交后等待它们归零再删除。
            } finally {
                rwLock.readLock().unlock();
            }
            if (inputs.size() < config.compactionThreshold()) {
                    return CompactionResult.skipped(CompactionStatus.NOTHING_TO_COMPACT,
                            inputs.size(), plan.totalRows(),
                            "活动段数 " + inputs.size() + " 未达压实阈值 "
                                    + config.compactionThreshold());
                }
                // 校验连续性
                long expect = 0L;
                for (SegmentEntry e : inputs) {
                    if (e.seqStart() != expect) {
                        throw new StoreException(StoreException.Reason.BAD_FORMAT,
                                "压实前发现段序号不连续，段 " + e.segmentId());
                    }
                    expect = e.seqEnd();
                }

                long newSegId = segmentIdCounter.getAndIncrement();
                long totalRows = inputs.stream().mapToLong(SegmentEntry::rowCount).sum();
                long seqStart = inputs.get(0).seqStart();
                long seqEnd = inputs.get(inputs.size() - 1).seqEnd();
                int schemaVersionAtBuild = currentSchema.version();
                staging = ManifestStore.stagingSegmentFile(config.dataDir(), newSegId);

                log.info("压实开始 输入段={} 行数={} 输出段={}", inputs.size(), totalRows, newSegId);
                ResourceLimiter.Permit outPermit = fileLimiter.acquire();
                try {
                    SegmentWriter writer = SegmentWriter.create(staging, newSegId,
                            seqStart, seqEnd, (int) Math.min(Integer.MAX_VALUE, totalRows),
                            schemaVersionAtBuild);
                    try {
                        // 逐列流式合并：任意时刻只持有「全表一列 + 当前段一列」与 2 个打开文件
                        for (ColumnDef col : currentSchema.columns()) {
                            List<Object> merged = new ArrayList<>((int) Math.min(Integer.MAX_VALUE, totalRows));
                            for (SegmentEntry in : inputs) {
                                ResourceLimiter.Permit inPermit = fileLimiter.acquire();
                                SegmentReader reader;
                                try {
                                    reader = SegmentReader.open(
                                            ManifestStore.segmentFile(config.dataDir(), in.segmentId()), inPermit);
                                } catch (RuntimeException ex) {
                                    inPermit.close();
                                    throw ex;
                                }
                                try {
                                    ChunkIndex ci = null;
                                    for (ChunkIndex x : reader.chunks()) {
                                        if (x.columnId() == col.id()) {
                                            ci = x;
                                            break;
                                        }
                                    }
                                    if (ci == null || col.id() >= in.schemaVersion()) {
                                        for (int i = 0; i < in.rowCount(); i++) {
                                            merged.add(null);
                                        }
                                    } else {
                                        merged.addAll(reader.readColumn(ci, col.type()));
                                    }
                                } finally {
                                    reader.close(); // 同时释放 inPermit
                                }
                            }
                            if (merged.size() != totalRows) {
                                throw new StoreException(StoreException.Reason.BAD_FORMAT,
                                        "压实合并列 " + col.name() + " 行数 " + merged.size()
                                                + " 与输入总行数 " + totalRows + " 不符");
                            }
                            writer.writeChunk(col, merged);
                        }
                    } finally {
                        writer.close();
                    }
                } finally {
                    outPermit.close();
                }

                // 注入点：staging 已完整落盘但尚未发布，模拟「压实中途失败」
                Runnable hook = this.compactionFailureHook;
                if (hook != null) {
                    hook.run();
                }

                // 提交：写锁内仅复核输入仍在清单、rename、发布清单、提取旧段 id。
                // 绝不在持写锁时等待旧段引用归零，否则与「读锁登记引用」形成循环等待（死锁）。
                List<Long> oldIds;
                int before;
                rwLock.writeLock().lock();
                try {
                    Manifest current = this.manifest;
                    before = current.segments().size();
                    Set<Long> currentIds = new HashSet<>();
                    for (SegmentEntry e : current.segments()) {
                        currentIds.add(e.segmentId());
                    }
                    for (SegmentEntry in : inputs) {
                        if (!currentIds.contains(in.segmentId())) {
                            throw new StoreException(StoreException.Reason.IO_ERROR,
                                    "压实提交时发现输入段 " + in.segmentId() + " 已不在清单中，放弃提交");
                        }
                    }
                    Path target = ManifestStore.segmentFile(config.dataDir(), newSegId);
                    try {
                        Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
                    } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                        Files.move(staging, target);
                    }
                    Set<Long> inputIds = new HashSet<>();
                    for (SegmentEntry e : inputs) {
                        inputIds.add(e.segmentId());
                    }
                    List<SegmentEntry> remaining = new ArrayList<>();
                    for (SegmentEntry e : current.segments()) {
                        if (!inputIds.contains(e.segmentId())) {
                            remaining.add(e);
                        }
                    }
                    List<String> mergedBatchIds = new ArrayList<>();
                    for (SegmentEntry in : inputs) {
                        mergedBatchIds.addAll(in.batchIds());
                    }
                    SegmentEntry compactedEntry = new SegmentEntry(newSegId, seqStart, seqEnd,
                            (int) Math.min(Integer.MAX_VALUE, totalRows),
                            schemaVersionAtBuild, List.copyOf(mergedBatchIds));
                    remaining.add(compactedEntry);
                    remaining.sort((a, b) -> Long.compare(a.seqStart(), b.seqStart()));

                    Manifest published = current.replaced(current.columns(), remaining,
                            segmentIdCounter.get(), current.nextSeq());
                    manifestStore.write(published);
                    this.manifest = published;
                    committed = true;
                    oldIds = new ArrayList<>(inputIds);
                    // 先登记待删，使任何在锁窗口内的新引用路径都不会再选到它们
                    synchronized (refsLock) {
                        pendingDelete.addAll(oldIds);
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }

                // 锁外等待在途读者结束并物理删除：读方登记引用需要读锁，写锁已释放，可正常完成
                synchronized (refsLock) {
                    waitForRefsDrain(oldIds);
                }
                for (Long id : oldIds) {
                    deleteOrQuarantine(id);
                }
                synchronized (refsLock) {
                    pendingDelete.removeAll(oldIds);
                    oldIds.forEach(segmentRefs::remove);
                    refsLock.notifyAll();
                }
                long elapsed = (System.nanoTime() - startNanos) / 1_000_000L;
                Manifest after = this.manifest;
                log.info("压实完成 输入段={} 输出段={} 当前段数={} 行数不变={} 耗时={}ms",
                        oldIds.size(), newSegId, after.segments().size(), totalRows, elapsed);
                return CompactionResult.compacted(before, after.segments().size(),
                        totalRows, newSegId, elapsed);
        } catch (StoreException e) {
            if (!committed) {
                deleteQuietly(staging);
            }
            log.warn("压实失败：清单未变更，已有数据可读；原因={} 信息={}", e.reason(), e.getMessage());
            throw e;
        } catch (Exception e) {
            deleteQuietly(staging);
            throw new StoreException(StoreException.Reason.IO_ERROR, "压实失败", e);
        } finally {
            compacting.set(false);
        }
    }

    @Override
    public Schema schema() {
        checkOpen();
        return currentSchema;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.info("存储关闭");
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new StoreException(StoreException.Reason.STORE_CLOSED, "存储已关闭");
        }
    }

    /** 测试支持：设置压实提交前故障注入钩子（null 清除）。 */
    public void setCompactionFailureHook(Runnable hook) {
        this.compactionFailureHook = hook;
    }

    public ResourceLimiter resourceLimiter() {
        return fileLimiter;
    }

    // ---------- 引用计数与段删除 ----------

    private void acquireRefs(List<SegmentEntry> entries) {
        for (SegmentEntry e : entries) {
            segmentRefs.computeIfAbsent(e.segmentId(), k -> new AtomicInteger())
                    .incrementAndGet();
        }
    }

    private void releaseRefs(List<SegmentEntry> entries) {
        synchronized (refsLock) {
            for (SegmentEntry e : entries) {
                AtomicInteger ai = segmentRefs.get(e.segmentId());
                if (ai != null) {
                    int v = ai.decrementAndGet();
                    if (v < 0) {
                        throw new IllegalStateException("段引用计数为负: " + e.segmentId());
                    }
                }
            }
            refsLock.notifyAll();
        }
    }

    private void waitForRefsDrain(List<Long> ids) {
        // 调用方已持 refsLock 与写锁：提交后不可能再有新读者引用这些段
        while (true) {
            boolean allZero = true;
            for (Long id : ids) {
                AtomicInteger ai = segmentRefs.get(id);
                if (ai != null && ai.get() > 0) {
                    allZero = false;
                    break;
                }
            }
            if (allZero) {
                return;
            }
            try {
                refsLock.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException(StoreException.Reason.INTERRUPTED,
                        "等待旧段读取结束时被中断", e);
            }
        }
    }

    private void deleteOrQuarantine(long segmentId) {
        Path f = ManifestStore.segmentFile(config.dataDir(), segmentId);
        try {
            Files.deleteIfExists(f);
            return;
        } catch (IOException e) {
            log.warn("旧段删除失败，移入 orphan 目录: {} ({})", f, e.toString());
        }
        try {
            Path orphan = config.dataDir().resolve("orphan");
            Files.createDirectories(orphan);
            Files.move(f, orphan.resolve(f.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e2) {
            // 清单已不再引用该文件；残留不影响正确性，下次启动恢复会再次隔离
            log.warn("旧段隔离也失败，留待下次启动清理: {} ({})", f, e2.toString());
        }
    }

    private static void deleteQuietly(Path p) {
        if (p == null) {
            return;
        }
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // staging 清理失败由下次启动恢复统一删除 *.tmp
        }
    }
}
