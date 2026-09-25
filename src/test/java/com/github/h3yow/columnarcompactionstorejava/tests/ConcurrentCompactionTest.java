package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionStatus;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发与失败语义：
 * 1) 压实进行中多个读取方并发扫描，每个快照必须自洽（行数 == snapshotSeq、id 全局递增无重复）；
 * 2) 压实提交前注入失败：已有数据与清单不变、无 staging 残留，之后压实仍可成功；
 * 3) 压实进行中重复触发：得到 ALREADY_RUNNING，不产生重复段。
 */
class ConcurrentCompactionTest {

    private static Row row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", "n" + id);
        return new Row(m);
    }

    @Test
    void concurrentReadsDuringCompactionAlwaysSeeConsistentSnapshot(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir).withMaxOpenFiles(32).withCompactionThreshold(2);
        DiskColumnarStore store = DiskColumnarStore.open(cfg);

        final int initial = 6;
        for (int i = 0; i < initial; i++) {
            assertTrue(store.write(new WriteBatch("w" + i, List.of("id", "name"),
                    List.of(row(i * 10L), row(i * 10L + 1), row(i * 10L + 2)))).accepted());
        }

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger snapshotChecks = new AtomicInteger();
        AtomicInteger writerId = new AtomicInteger(initial);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch start = new CountDownLatch(1);

        // 3 个读取线程：不断取快照并强校验
        for (int t = 0; t < 3; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (!stop.get()) {
                        ScanResult scan = scanWithRetry(store);
                        List<Row> rows = scan.rows();
                        // 不变量 1：行数 == snapshotSeq（段行数合计）
                        assertEquals(scan.snapshotSeq(), rows.size(),
                                "快照行数必须等于序号上界");
                        // 不变量 2：段 id 列表与行数一致，且段按 seq 连续 -> id 严格递增、无重复
                        long prev = -1L;
                        for (Row r : rows) {
                            long id = (Long) r.values().get("id");
                            assertTrue(id > prev, "快照内行序必须严格递增（无重复/错序）: "
                                    + prev + " -> " + id);
                            // 每一行必须带全部当前列（缺失列以 null 稳定填充，而不是错列）
                            assertTrue(r.values().containsKey("name"));
                            prev = id;
                        }
                        snapshotChecks.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 1 个压实线程
        pool.submit(() -> {
            try {
                start.await();
                while (!stop.get()) {
                    store.compact();
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 1 个写入线程
        pool.submit(() -> {
            try {
                start.await();
                int n;
                while ((n = writerId.getAndIncrement()) < initial + 20) {
                    store.write(new WriteBatch("w" + n, List.of("id", "name"),
                            List.of(row(n * 10L), row(n * 10L + 1), row(n * 10L + 2))));
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        Thread.sleep(2500);
        stop.set(true);
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        int totalBatches = initial + 20;
        ScanResult fin = store.scan();
        assertEquals(totalBatches * 3L, fin.rows().size(), "最终行数 = 全部成功写入批次之和，无丢失/重复");
        System.out.println("[并发快照校验] 期间通过自洽校验的快照数=" + snapshotChecks.get()
                + " 最终行数=" + fin.rows().size());
        assertTrue(snapshotChecks.get() > 50, "应已观察到大量完整快照");
        store.close();
    }

    private static ScanResult scanWithRetry(DiskColumnarStore store) {
        // 文件许可暂时不足时允许等待重试；任何其它异常都视为快照不一致直接抛出
        for (int i = 0; i < 10; i++) {
            try {
                return store.scan();
            } catch (StoreException e) {
                if (e.reason() == StoreException.Reason.RESOURCE_TIMEOUT) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("持续无法获取文件许可");
    }

    @Test
    void failedCompactionLeavesIntactDataAndNoHalfArtifacts(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir).withCompactionThreshold(2);
        DiskColumnarStore store = DiskColumnarStore.open(cfg);
        for (int i = 0; i < 3; i++) {
            assertTrue(store.write(new WriteBatch("f" + i, List.of("id", "name"),
                    List.of(row(100L + i)))).accepted());
        }
        long rowsBefore = store.scan().rows().size();
        int segBefore = store.scan().segmentIds().size();

        // 注入：staging 段写完、清单发布前抛错
        store.setCompactionFailureHook(() -> {
            throw new RuntimeException("模拟压实中途失败（断电/异常）");
        });
        StoreException thrown = null;
        try {
            store.compact();
        } catch (StoreException e) {
            thrown = e;
        }
        System.out.println("[失败压实] 抛出原因=" + (thrown == null ? null : thrown.reason())
                + " 信息=" + (thrown == null ? null : thrown.getMessage()));
        assertTrue(thrown != null, "中途失败必须向上抛出可区分异常");

        ScanResult mid = store.scan();
        assertEquals(rowsBefore, mid.rows().size(), "失败压实后行数不得变化");
        assertEquals(segBefore, mid.segmentIds().size(), "失败压实后旧段必须全部仍在清单");
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> tmps = s.filter(p -> p.getFileName().toString().endsWith(".tmp")).toList();
            System.out.println("[残留检查] .tmp 文件=" + tmps);
            assertTrue(tmps.isEmpty(), "不得留下半成品 staging 文件");
        }

        // 清除故障后压实必须成功，且数据一致
        store.setCompactionFailureHook(null);
        CompactionResult ok = store.compact();
        assertEquals(CompactionStatus.COMPACTED, ok.status());
        assertEquals(rowsBefore, ok.rowsAfter());
        ScanResult after = store.scan();
        assertEquals(rowsBefore, after.rows().size());
        assertEquals(1, after.segmentIds().size());
        store.close();
    }

    @Test
    void concurrentCompactionTriggerIsDeduped(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir).withCompactionThreshold(2);
        DiskColumnarStore store = DiskColumnarStore.open(cfg);
        for (int i = 0; i < 4; i++) {
            assertTrue(store.write(new WriteBatch("d" + i, List.of("id", "name"),
                    List.of(row(200L + i), row(210L + i)))).accepted());
        }

        // 在压实提交前（compacting=true 期间）从另一线程重复触发，必须得到 ALREADY_RUNNING
        AtomicInteger alreadyRunning = new AtomicInteger();
        store.setCompactionFailureHook(null);
        // 用一个在构建完成后、发布前触发的钩子发起并发 compact()
        store.setCompactionFailureHook(() -> {
            Thread other = new Thread(() -> {
                CompactionResult r = store.compact();
                System.out.println("[并发压实触发] status=" + r.status() + " 依据=" + r.detail());
                if (r.status() == CompactionStatus.ALREADY_RUNNING) {
                    alreadyRunning.incrementAndGet();
                }
            });
            other.start();
            try {
                other.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        CompactionResult main = store.compact();
        assertEquals(CompactionStatus.COMPACTED, main.status());
        assertEquals(1, alreadyRunning.get(), "重复触发必须被识别为 ALREADY_RUNNING 且不执行");

        ScanResult after = store.scan();
        assertEquals(8, after.rows().size(), "重复触发不得重复计数");
        assertEquals(1, after.segmentIds().size(), "重复触发不得产生重复段");
        store.close();
    }
}
