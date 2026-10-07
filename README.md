# columnar-compaction-store-java

本地磁盘列式存储：批量写入、一致快照读取、压实（compaction）、**带保留期的版本快照**。

## 基本能力

- `ColumnarStore.open(dir, schema, limiter[, waitMillis, snapshotLimits, clock])` 打开/创建存储。
- `write(Batch)` 整批校验、整批落盘；segment 文件 tmp+fsync+原子 rename，manifest 原子替换。
- `readSnapshot()` 读取当前一致快照（manifest 级别的原子视图）。
- `compact()` 把全部 live segment 合并为一个；串行、幂等、崩溃安全。
- `addColumn` / `evolveTo` 只允许追加列，老行读回时补类型默认值。

## 版本快照（VersionSnapshot）

```java
VersionSnapshot snap = store.openSnapshot();
List<Map<String, Object>> rows = snap.read(); // 任意时刻重复读，结果恒定
snap.close();                                  // 释放占用，幂等
```

语义：

- 打开那一刻的 **数据版本**（manifest `version`）被钉住：之后无论发生多少写入、
  多少次压实、多少轮过期清理，`snap.read()` 返回的逻辑数据都与打开时完全一致，
  且可重复读取。schema 也固定在打开时的版本。
- 实现方式：快照钉住一个不可变 manifest，并对它引用的 segment 文件做引用计数；
  被钉住的文件不会被压实或清理删除。
- 快照是**进程内**的，随所属 `ColumnarStore` 实例存活；store 关闭后快照不可再读。

失败语义（可区分，绝不静默返回空值/错列数据）：

| 场景 | ErrorCode |
|---|---|
| 读已 `close()` 的快照 | `SNAPSHOT_CLOSED` |
| 所属 store 已关闭后读 | `STORE_CLOSED` |
| 钉住的 segment 文件意外丢失（如外部删除） | `SNAPSHOT_EXPIRED` |
| 打开时超出快照数量/钉住字节上限 | `SNAPSHOT_LIMIT_EXCEEDED` |

资源上限与释放：

- `SnapshotRegistry.SnapshotLimits(maxSnapshots, maxPinnedBytes)` 限制并发快照数和
  它们钉住的磁盘字节总量；默认 `(64, 1GiB)`。
- `openSnapshot()` 在限时内等待空位（排队），超时抛 `SNAPSHOT_LIMIT_EXCEEDED`，
  消息里带具体哪个上限被打满。
- `close()` 后名额与字节预算立即归还（`openSnapshotCount()` /
  `snapshotPinnedBytes()` 可观测）；仅被快照钉住的 segment 文件在最后一个
  快照关闭时被真正删除。

## 保留期与过期清理（Retention）

```java
store.enforceRetention(RetentionPolicy.maxAge(Duration.ofDays(7))); // 只留最近 7 天
store.enforceRetention(RetentionPolicy.keepLastVersions(3));        // 只留最近 3 个版本
```

语义：

- 过期判定以 **segment 粒度** 进行：每个 segment 记录创建时间（`createdAt`），
  每次写入产生一个新版本；压实产生合并 segment，其创建时间取**输入中最老的一个**，
  因此按年龄清理看到的仍是数据的真实年龄。
- 清理分两步：先把过期 segment 从 manifest 原子摘除（新读不再看到），再删除文件。
  被打开快照钉住的过期文件会**延期**到快照关闭时再删——快照语义优先于立即释放磁盘。
- 返回 `RetentionResult(expired, reclaimed, deferredToSnapshots)`，可观测本轮摘除了
  多少、真正删了多少、多少因快照延期。

并发与失败行为：

- 清理与压实在同一把锁下串行；与写入、读取、快照打开/关闭可安全并发。
- 可安全中断：manifest 摘除是原子的；中断只会留下待删文件，绝不会丢 live 数据、
  不会出现"读一半"的状态。待删集合在摘除前已登记，且删除前会复核"不在 live
  manifest、未被钉住"，因此中断后任何后续 drain（重试清理、压实、快照关闭）都会
  把空间真正回收。
- 幂等：重复触发时若 manifest 已无过期 segment，则为无操作（不推进版本号、不改数据、
  不重复计数），只顺带完成此前中断遗留的文件回收。已清理的数据重开存储后也不会复活。

## 兼容范围

- **旧 manifest**（segment 为纯字符串数组、无 `version` 字段）可直接加载：
  `version` 视为 0，老 segment 的创建时间戳记为**加载时刻**——升级后老数据不会被
  立刻误判过期，之后随真实年龄正常过期。下次 manifest 保存时自动写成新格式。
- **segment 文件格式未变**（`CCS1`），新旧代码都能读写既有数据文件。
- 快照是运行期概念，不落盘；进程重启后旧快照句柄不存在，重启时孤立的非 live
  segment 文件由 `cleanupOrphans` 统一回收。

## 本地验证

```bash
mvn test
```

关键用例（日志中带 `PASS` 与 `basis:` 说明，可直接 grep）：

- `VersionSnapshotTest` — 快照一致性（跨写入/压实/清理）、可重复读、schema 钉住、
  关闭后/关库后/文件丢失的可区分失败、数量与字节上限、排队等待、释放后空间回收。
- `RetentionCleanupTest` — 按年龄/按版本数过期、磁盘真正释放、幂等与版本号不空转、
  中断后重试完成清理、过期但被钉住的文件延期删除、清理+压实+读取并发交错、
  旧格式 manifest 兼容。
- 回归：`BatchValidationTest`、`CompactionTest`、`ResourceAndCorruptionTest`、
  `SchemaEvolutionTest` 全部保持通过。
