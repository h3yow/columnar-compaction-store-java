# Columnar Compaction Store（带版本快照与保留期）

本地磁盘列式存储：批量写入、全量一致读取、压实（compaction）、**显式版本快照**与**按保留期过期清理**。
纯 Java 21 + 单目录存储，无外部依赖。

---

## 1. 数据模型与文件布局

每个 store 就是一个目录：

| 文件 | 说明 |
| --- | --- |
| `manifest.json` | 唯一真相源：当前 schema、当前 live 段（有序）、压实代号（`compactionGeneration`）、**版本目录（`versions`）** |
| `seg-NNNNNN.ccs` | 不可变列式段文件（带 CRC32，`tmp + fsync + 原子 rename` 落盘） |
| `*.tmp` | 写入/压实中途的临时文件，崩溃后下次打开即回收 |

每次成功的 **写入** 或 **压实** 都原子地产生一个新版本（`VersionInfo`）：

```
versionId            单调递增
createdAtMillis      版本可见时刻（注入的 Clock，便于测试时间型保留）
compactionGeneration 该时刻的压实代号
schemaVersion/columnCount 该时刻的 schema（schema 只允许追加列）
segments             该时刻 live 段文件名的有序列表（快照读的就是这些）
```

manifest 的每次更新都是「写 tmp → fsync → 原子 rename」，崩溃只会留下旧版或新版，绝不会出现半截状态。

---

## 2. 版本快照语义

```java
// 打开当前 head 版本
try (VersionSnapshot snap = store.openSnapshot()) {
    ColumnarStore.Snapshot view = snap.read();   // schema + rows
}

// 打开保留窗口内的任意历史版本
try (VersionSnapshot old = store.openSnapshot(versionId)) { ... }
```

- **一致性**：快照固定一个版本的段集合与当时 schema。此后无论发生多少次写入、压实、过期清理，
  `snap.read()` 返回的逻辑数据与打开那一刻**逐行一致**，不会混入新数据，也不会少记录或读到半截。
- **物理保护（pin）**：快照打开期间，其引用的段被 refcount 钉住；清理只会删除
  「没有任何保留版本、live 集合或打开中的快照引用」的段。多个快照共享同一段时字节只计一次。
- **schema 演进**：快照返回打开时刻的 schema（列前缀）；之后追加的列不会泄漏进旧快照；
  旧行在当前读视图里用类型默认值补齐（int 0 / long 0 / double 0.0 / bool false / string ""）。
- **资源归属**：快照是**进程内句柄**（pin 表在内存中），版本目录在 manifest 里持久化；
  进程重启后历史版本仍可重新打开，但重启前未关闭的句柄不延续。
- **释放**：`close()` 幂等；释放后 pin 计数与字节占用立即归还。释放后再读得到
  `SNAPSHOT_CLOSED`。

### 打开/读取失败时的可区分错误码（绝不静默返回空或错列数据）

| ErrorCode | 触发场景 |
| --- | --- |
| `SNAPSHOT_VERSION_EXPIRED` | 版本已超出保留窗口（或从不存在），无法打开 |
| `SNAPSHOT_DATA_UNAVAILABLE` | 版本在目录内但其段文件缺失（如被外部删除），打开前 stat 或读取时发现，无法再保证一致性 |
| `SNAPSHOT_LIMIT_EXCEEDED` | 同时打开的快照数超过上限，等待超时 |
| `SNAPSHOT_BYTES_LIMIT_EXCEEDED` | 所有打开快照 pin 住的段字节并集超过预算，等待超时（单版本本身超预算则立即拒绝） |
| `SNAPSHOT_CLOSED` | 对已释放快照执行读取 |
| `CORRUPT_SEGMENT` | 段文件存在但 magic/长度/CRC 校验失败（原有能力） |

---

## 3. 保留期与过期清理

```java
RetentionPolicy.currentOnly()                  // 默认：只保留当前 head（等价于升级前行为）
RetentionPolicy.keepVersions(K)                // 只保留最近 K 个版本
RetentionPolicy.ofAge(maxAgeMillis, K)         // 同时要求：版本龄期 ≤ maxAge 且在最近 K 个内
```

两个条件取**交集**，保留集永远是版本历史（最旧在前）的一个**后缀**：要么整版本可达，
要么整版本从目录移除，不存在"半保留"。**head（当前可见表）永远不会被过期**——保留期管的是
"还能打开多久的历史"，从不是当前表本身。

```java
ColumnarStore.RetentionResult r = store.enforceRetention();
// r.expiredVersions() 本次从目录过期的版本数
// r.deletedFiles() / r.deletedBytes() 真正从磁盘删除的文件数与字节数
```

### 空间何时真正释放

一个段文件被物理删除，当且仅当它**同时**不被：当前 live 集合、任何保留版本、任何打开的快照引用。
因此：

- 纯追加写入（未压实）时，旧段仍构成当前表，保留期再激进也不会删它们；**压实之后**，
  旧段脱离 live 集合，才可能随版本过期而释放。
- 默认 `currentOnly` 策略下，压实提交后的旧段在同一次操作的回收阶段即被删除，
  与升级前的磁盘占用行为一致。

### 清理的崩溃安全、可中断与幂等

1. **先提交目录裁剪（原子 manifest），再删文件**。崩溃发生在提交前：什么都没变；
   崩溃发生在提交后：目录已经一致，删文件只是纯垃圾回收，下次清理继续。
2. 已从目录移除的版本**不可能复活**（段文件名永不复用其逻辑身份，且只有 manifest 引用才能让数据可见）。
3. 删除循环响应线程中断：中断点在提交之后；中断时抛 `CLEANUP_INTERRUPTED`（可区分、可重试），
   已删的保持已删，未删的下次补删。
4. 重复触发 `enforceRetention()` 是幂等的：第二次过期 0 个版本、删除 0 个文件。
5. 单个文件删除失败（如平台锁）会跳过留给下一轮，不影响其他文件与目录一致性。
6. 每次写入/压实提交后会做一次**尽力而为**的后台回收；其异常不会让已经成功的写入失败，
   显式 `enforceRetention()` 负责把任何残留清干净。

---

## 4. 快照配额（资源上限）

```java
new SnapshotLimits(maxOpenSnapshots, maxPinnedBytes);
// 默认：16 个并发快照；pin 字节并集 256 MiB
```

- 超限时调用会**排队等待**一段时间（`open(..., resourceWaitMillis, ...)` 中的等待窗口），
  期间其它快照释放会唤醒等待者；超时后按上表中的不同错误码拒绝。
- 计数口径：快照数 = 打开句柄数；pin 字节 = 所有打开快照所引用段文件大小的**并集**
  （共享段只计一次）。被拒绝的打开不会占用任何计数。
- 释放句柄后计数与字节立刻归还；清理、压实、释放以任意顺序反复交错，
  计数不会重复扣减，空间不会迟迟不回收（有并发压力测试守护）。

---

## 5. 并发模型

- `lifecycleLock`（公平）串行化所有拓扑变更：写入、压实、保留清理、快照打开。
  这保证「检查版本存在/段存在 → 建立 pin」与「计算删除集」不会互相穿插：
  清理永远不可能删掉一个正在建立中的合法快照所需的段。
- 压实另由 `compactionLock` 串行，多个并发压实只有一个真正合并（既有语义）。
- 快照**读取**不持生命周期锁：快照读的是已 pin 的不可变段，与后续写入/压实完全并行。
- 内存预算与打开文件数由 `ResourceLimiter`（既有能力）约束，超限 `RESOURCE_LIMIT_EXCEEDED`。

---

## 6. 兼容范围（老数据 / 旧文件）

- 旧版写出的 `manifest.json` 没有 `versions` 字段：打开时**一次性升级**——
  把当前 live 集合合成为 `versionId=1`（时间戳取 manifest 文件 mtime）并原子重写，
  之后即按新版本目录运作。旧段文件格式（`CCS1`）完全不变。
- 新代码读取旧文件、旧段，无需迁移数据。
- 旧 API（`write / readSnapshot / compact / addColumn / evolveTo`、`open(dir, schema, limiter)`）
  签名保持不变；旧调用方默认得到 `currentOnly` 保留策略，行为与升级前一致。
- schema 仍只允许追加列；rename/drop/retype 继续以 `SCHEMA_INCOMPATIBLE` 拒绝。
- 升级前遗留在目录里的 `.tmp` 与未引用段，仍在打开时按原规则清理。

---

## 7. 本地验证

```bash
# 全量单测（原有 28 个 + 新增版本快照 15 个 = 43 个）
mvn test

# 只跑新增场景
mvn test -Dtest=VersionSnapshotTest

# 查看结构化场景日志（每个用例都以 [snap-stable]/[snap-expire]/[snap-age]/... 标注覆盖点）
grep -E "\[(snap|interleave|queue|interrupt|mix|cap|closed|retry|reopen|schema|unavailable)" \
  target/surefire-reports/*VersionSnapshotTest*
```

`VersionSnapshotTest` 覆盖：

1. `snapshotStaysConsistent...` — 快照跨写入/压实/清理多次重读完全一致，不被污染；
2. `expiredVersionIsRejectedAndItsFilesAreReallyDeleted` — 过期版本打开被拒、段被真删、重复清理幂等不复活；
3. `ageBasedRetentionDeletesDataOlderThanTheWindow` — 按天龄过期，真实释放磁盘字节；
4. `pinnedSnapshotProtectsItsFilesFromCleanup` — 有效快照钉住文件，激进清理也不能删，释放后立即回收；
5. `snapshotCountLimit...` / `pinnedBytesLimit...` — 数量/字节上限的可区分拒绝、排队、共享只计一次、释放归还；
6. `closedSnapshotReadsAreRejected` — 释放后读取得到 `SNAPSHOT_CLOSED`；
7. `longRunningSnapshotStaysCorrectUnderConcurrentWritesCompactionAndCleanup` —
   长查询快照与后台写入+压实+清理并发交错，数百次读取零偏差、计数归零；
8. `snapshotCapQueuesAndAcceptsAfterConcurrentRelease` — 超限排队等待，释放后获得；
9. `interruptedCleanupIsReportedDistinctlyAndRetryFinishesIt` —
   清理在删除阶段被中断 → `CLEANUP_INTERRUPTED`，重试删净，再跑为空操作；
10. `repeatedMixOfCompactionRetentionAndReleaseIsStableAndIdempotent` —
    压实/清理/打开/释放混合 6 轮，计数稳定、结果幂等；
11. `failedRetentionCommitLeavesDataIntactAndRetrySucceeds` — 提交前崩溃数据完好，重试成功；
12. `snapshotReadAfterExternalDataLoss...` — 外部丢文件时读取得 `SNAPSHOT_DATA_UNAVAILABLE`；
13. `versionsPersistAcrossReopenAndOldManifestIsUpgraded` — 版本目录跨重启持久化、旧 manifest 自动升级；
14. `snapshotOfVersionWithSchemaEvolutionReturnsOriginalColumns` — 旧快照不泄漏后来追加的列。

### 快速手工体验

```java
Clock clock = Clock.systemUTC();
RetentionPolicy retention = RetentionPolicy.ofAge(7L * 24 * 60 * 60 * 1000, 1000); // 最近 7 天且最多 1000 版
SnapshotLimits caps = new SnapshotLimits(64, 4L << 30);                            // 64 个快照 / 4 GiB pin
ColumnarStore store = ColumnarStore.open(dir, schema,
        new ResourceLimiter(256L << 20, 64), 1_000, retention, caps, clock);

store.write(new Batch(rows));
try (VersionSnapshot snap = store.openSnapshot()) {        // 长分析任务开始
    // ... 期间其它线程持续 write/compact/enforceRetention ...
    snap.readRows();                                       // 始终是打开时的数据
}
store.enforceRetention();                                  // 需要时立即执行过期与空间回收
```

