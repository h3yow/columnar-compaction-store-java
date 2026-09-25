# 基于本地磁盘的列式存储（写入 / 读取 / 压实）

一套面向「按批次写入、全量快照读取、后台压实」场景的单进程磁盘列式存储。
目标是在**写入端 / 读取端 / 压实端并发与异常中断**下，都能得到**一致且可解释**的结果：
不静默丢弃任何坏数据、不静默错列、不暴露半成品快照、重复触发压实不重复计数。

## 1. 快速开始

```java
StoreConfig config = StoreConfig.defaults(Path.of("./data"));
try (ColumnarStore store = DiskColumnarStore.open(config)) {

    // 写入：自包含批次（列头 + 若干行）
    WriteResult wr = store.write(new WriteBatch(
            "batch-001",
            List.of("id", "name", "active"),
            List.of(
                new Row(Map.of("id", 1L, "name", "alice", "active", true)),
                new Row(Map.of("id", 2L, "name", null, "active", false)), // 缺字段显写 null
            )));
    if (!wr.accepted()) {
        wr.rejection().reason(); // 可区分的拒绝原因，见 §3
    }

    // 读取：得到一个完整快照
    ScanResult snapshot = store.scan();
    snapshot.rows();        // 全局行序（按 seq）
    snapshot.snapshotSeq(); // 该快照可见的最大序号（排他上界）== 行数
    snapshot.segmentIds();  // 构成快照的段，便于解释与校验

    // 压实：合并全部活动段为 1 个，逻辑数据不变
    CompactionResult cr = store.compact();
}
```

HTTP 入口（可选，Spring Boot）：

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/api/store/write`  | 提交批次；拒绝也返回 `accepted:false` + 原因（是业务结果，不是 5xx） |
| GET  | `/api/store/scan`   | 全量快照读取 |
| POST | `/api/store/compact` | 手动触发压实 |

## 2. 文件格式

数据目录（默认 `./data`）：

```
data/
├── manifest.json        # 清单：schema + 活动段列表 + 计数器
├── manifest.json.tmp    # 仅在写清单的瞬间存在（崩溃残留会在启动时删除）
├── seg-000001.dat       # 不可变列时段
├── seg-000002.dat
├── seg-NNNNNN.dat.tmp   # 段的 staging（rename 前）；启动时一律删除
└── orphan/              # 无法安全删除 / 未被引用的段，隔离审计而非静默丢弃
```

### 2.1 段文件 `seg-NNNNNN.dat`（大端）

```
文件头 (40 字节)
  int  MAGIC        = 'C''C''S''G'
  int  VERSION      = 1
  long segmentId
  long seqStart     （包含）
  long seqEnd       （排他）
  int  rowCount
  int  schemaVersion   写入该段时的列数；读取端据此判断旧段缺哪些新列
  int  chunkCount      （保留占位，当前为 0，真实块数以尾部为准）

列块（每个出现的列一块，按 columnId 升序）
  int  columnId
  int  rows
  int  nulls
  int  payloadLength
  byte[payloadLength] payload
        byte[rows] nullFlags(0/1)
        非 null 值按行序：LONG/DOUBLE=8B；BOOLEAN=1B；STRING=int utfLen + UTF-8
  int  crc32           对 columnId..payload 全部字节的 CRC32

文件尾（文件最后部分）
  重复 N 次 { int columnId; int rows; int nulls; long offset; long length }
  int  indexEntryCount
  int  TRAILER_MAGIC = 'C''C''T''R'
```

- **段不可变**：一旦随清单发布，绝不原地修改。压实也是写新段、原子替换。
- **每个列块带 CRC32**；打开时校验头部与尾部魔数，读列时校验 CRC。
  写入中断的文件没有完整尾部 → 明确报 `BAD_FORMAT`；字节被篡改 → 报 `CRC_MISMATCH`。
  两种情况**都不会静默跳过记录**。

### 2.2 清单 `manifest.json`

记录当前列定义（id/name/type）、活动段（segmentId、seqStart/seqEnd、rowCount、
schemaVersion、该段包含的 batchIds）、`nextSegmentId`、`nextSeq`、清单 `version`。

写协议（崩溃安全）：写 `manifest.json.tmp` → fsync → **原子 rename** 覆盖 →
fsync 目录。因此旧清单要么完整存在，要么完整被替换，读不到半个清单。

## 3. 兼容规则（schema 演进）

- **列 id 密集、只增不变**：第一列 id=0，新增列 id=当前列数；旧列 id 永不复用。
- **新增字段缺省语义确定且稳定：恒为 null。**
  段记录写入时的 `schemaVersion`，读取时按 **id 对齐**：对 `columnId >= 段版本`
  （旧段中不存在）的列，统一补 `nCopies(rowCount, null)`，与该列在当前 schema 的位置无关。
- **不支持改名 / 删列**：一个「新名字」只会被当作新增列追加，旧列原样保留。
  正因为按 id 而非按位置对齐，**列改名或删列不会让读取端静默错列**。
- 新列类型由该批次**首个非 null 值**确定（LONG/DOUBLE/BOOLEAN/STRING 严格匹配）；
  全为 null 无法定型 → 拒绝（`MALFORMED_BATCH`）。
- 段格式与清单格式各有 `VERSION`/`formatVersion`，遇到不支持的版本明确报错。

## 4. 批次校验与可区分拒绝原因

整批原子校验，任何一行不合格就**拒绝整批**（不产生文件、不改 schema、不消耗序号）：

| 原因 | 判定依据 |
| ---- | -------- |
| `MALFORMED_BATCH` | batchId 空；列头空/含空名/重名；行集合空；行为 null |
| `MISSING_FIELD` | 某行缺少列头声明的键（不允许隐式省略，只能显式 null） |
| `UNKNOWN_FIELD` | 某行出现列头未声明的键 |
| `TYPE_MISMATCH` | 值的 Java 类型与列类型不兼容（Long/Double/Boolean/String 严格对应） |
| `DUPLICATE_BATCH_ID` | batchId 已在已提交段中（幂等去重） |
| `BATCH_TOO_LARGE` | 批次估算字节数 > `maxWriteBatchBytes`，写入前即拒绝 |

拒绝结果 `BatchRejection(reason, column, detail)` 带关联列与人类可解释细节；
存储层错误另用 `StoreException.Reason` 区分：`IO_ERROR / BAD_FORMAT / CRC_MISMATCH /
RESOURCE_TIMEOUT / MEMORY_LIMIT_EXCEEDED / STORE_CLOSED / INTERRUPTED`。

## 5. 并发与资源约束

- **快照边界 = 清单原子替换**。`Manifest` 不可变并以 `volatile` 发布；
  读者在短暂读锁内快照清单并为段登记引用计数，随后无锁做磁盘 I/O。
  读者持有的段集合在整个扫描期间不变，看到的不是旧快照就是新快照，**没有半成品**。
- **写事务串行**（写锁）：校验 → 从清单权威 `nextSeq` 连续分配序号 → 写 staging 并
  fsync → rename → 发布清单。序号不留空洞；batchId 去重与新列类型裁决无竞态。
  耗时的读 I/O 与压实 I/O 都在锁外，只有提交瞬间占用锁。
- **引用计数 + 安全删除**：压实发布新清单后，先释放写锁，再等待在途读者引用归零，
  然后物理删除旧段；删除失败则移入 `orphan/`。绝不在持写锁时等待读者（会死锁）。
  压实不给自己登记引用（否则提交后等待归零会自我死锁）。
- **压实单实例**：`compacting` CAS 保证重复/并发触发直接返回 `ALREADY_RUNNING`，
  不会产生重复段或重复计数；段数未达阈值返回 `NOTHING_TO_COMPACT`；
  成功返回 `COMPACTED`，且 `rowsBefore == rowsAfter`。
- **资源上限**：
  - 内存：`maxWriteBatchBytes` 限制单批次缓冲，超限 `BATCH_TOO_LARGE` 拒绝；
    压实逐列流式合并，同时只保留「全表一列 + 当前段一列」，不一次性载入全表。
  - 打开文件数：`maxOpenFiles` 信号量（至少 2，满足 1 输入 + 1 输出）。
    超限操作**等待**，超过 `fileAcquireTimeoutMillis` 仍无许可则以
    `RESOURCE_TIMEOUT` 拒绝，许可关闭幂等、无泄漏。

## 5.1 异常中断语义

- **写入失败**（发布前任一阶段）：只删除 staging，清单与已有数据不变，不消耗 seq。
- **压实中途失败**：staging 被清理，清单未切换，旧段全部仍在且可读；
  之后可重新压实成功。测试通过提交前故障注入覆盖。
- **进程崩溃后重启**：删除所有 `*.tmp`（rename 前的半成品）；
  清单缺失才初始化为空；清单损坏则**拒绝启动**（不静默重置）；
  未被清单引用的 `.dat` 移入 `orphan/`；启动时校验段序号连续性、计数一致性与文件存在性。

## 6. 验证方法

```bash
mvn test
```

共 16 个用例，覆盖任务要求的关键场景（用例均在日志打印输入批次与判定依据）：

| 测试类 | 覆盖点 |
| ------ | ------ |
| `BasicReadWriteTest` | 批次写入/全量读取、跨批次连续 seq、新增列缺省 null、改名不错列 |
| `MalformedBatchTest` | 6 类拒绝原因逐一可区分；坏批次不写任何行、不改 schema |
| `CompactionTest` | **重复压实**：5→1 段、行数不变、物理旧段删除、再写再压不重复计数；新列 null 保持 |
| `ConcurrentCompactionTest` | **压实中并发读取**（多读者 + 写 + 压实持续 2.5s，每个快照自洽）；**中途失败压实**数据/清单不变且无 staging 残留；并发重复触发 = `ALREADY_RUNNING` |
| `ResourceLimitTest` | 打开文件占满后读取等待→`RESOURCE_TIMEOUT`，释放后恢复、无许可泄漏 |
| `RecoveryAndCorruptionTest` | 重启数据完整 + staging 清理；截断段→`BAD_FORMAT`；篡改字节→`CRC_MISMATCH`；清单损坏→拒绝启动 |

快照自洽性在并发测试中被强断言：`rows.size() == snapshotSeq`、
行 id 严格递增无重复、每行含全部当前列键。

## 7. 已知边界（按当前需求刻意简化）

- 全量扫描接口一次返回所有行到内存；超大结果集的游标式读取可作为后续扩展
  （段格式尾部索引已支持按列/按需读取）。
- batchId 去重集合随压实合并保留在清单中；超大规模历史 id 的分层索引未实现。
- 单进程并发；未提供多进程文件锁。
