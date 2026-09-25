package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.manifest.ManifestStore;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 崩溃恢复与损坏语义：
 * 1) 重启后数据完整可读；残留 *.tmp 半成品被清理；
 * 2) 段文件损坏（尾部缺失 / 内容被改导致 CRC 不符）必须明确报错，绝不静默跳过记录。
 */
class RecoveryAndCorruptionTest {

    private static Row row(long id, String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("name", name);
        return new Row(m);
    }

    @Test
    void reopenPreservesAllDataAndCleansStaging(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir).withCompactionThreshold(10);
        try (DiskColumnarStore store = DiskColumnarStore.open(cfg)) {
            for (int i = 0; i < 3; i++) {
                assertTrue(store.write(new WriteBatch("p" + i, List.of("id", "name"),
                        List.of(row(i, "v" + i)))).accepted());
            }
            // 压实产生合并段后再重开
            store.write(new WriteBatch("p3", List.of("id", "name"),
                    List.of(row(3, "v3"))));
        }

        // 伪造一个崩溃残留 staging
        Files.writeString(ManifestStore.stagingSegmentFile(dir, 999999L), "garbage");

        try (DiskColumnarStore reopened = DiskColumnarStore.open(cfg)) {
            ScanResult scan = reopened.scan();
            assertEquals(4, scan.rows().size(), "重启后行数必须完整");
            assertEquals("v0", scan.rows().get(0).values().get("name"));
            assertEquals("v3", scan.rows().get(3).values().get("name"));
            System.out.println("[重启恢复] 行数=" + scan.rows().size()
                    + " 段=" + scan.segmentIds() + " 清单版本=" + scan.manifestVersion());
        }
        try (var s = Files.list(dir)) {
            assertTrue(s.noneMatch(p -> p.getFileName().toString().endsWith(".tmp")),
                    "staging 残留必须在启动时清理");
        }
    }

    @Test
    void truncatedSegmentIsDetectedNotSilentlySkipped(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (DiskColumnarStore store = DiskColumnarStore.open(cfg)) {
            assertTrue(store.write(new WriteBatch("t1", List.of("id", "name"),
                    List.of(row(1L, "a"), row(2L, "b")))).accepted());
        }
        // 截断每个段文件，破坏尾部 -> 必须报 BAD_FORMAT，而不是读到 0 行
        Path seg = ManifestStore.segmentFile(dir, 1L);
        long len = Files.size(seg);
        try (var ch = Files.newByteChannel(seg, java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(len - 3);
        }
        StoreException ex = assertThrows(StoreException.class, () -> DiskColumnarStore.open(cfg).scan());
        System.out.println("[损坏检测-截断] 原因=" + ex.reason() + " 信息=" + ex.getMessage());
        // 打开阶段做完整性校验（文件存在但读取在 scan）；scan 解析尾部时报 BAD_FORMAT
        assertTrue(ex.reason() == StoreException.Reason.BAD_FORMAT
                || ex.reason() == StoreException.Reason.CRC_MISMATCH);
    }

    @Test
    void flippedPayloadByteCausesCrcMismatch(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir);
        try (DiskColumnarStore store = DiskColumnarStore.open(cfg)) {
            assertTrue(store.write(new WriteBatch("crc1", List.of("id", "name"),
                    List.of(row(1L, "corrupt-me-please-0000")))).accepted());
        }
        Path seg = ManifestStore.segmentFile(dir, 1L);
        byte[] bytes = Files.readAllBytes(seg);
        // 在文件中部（payload 区域）翻转一个字节，保留完整尾部
        int idx = 60;
        bytes[idx] ^= 0xFF;
        Files.write(seg, bytes);

        StoreException ex = assertThrows(StoreException.class, () -> {
            try (DiskColumnarStore store = DiskColumnarStore.open(cfg)) {
                store.scan();
            }
        });
        System.out.println("[损坏检测-CRC] 原因=" + ex.reason() + " 信息=" + ex.getMessage());
        assertEquals(StoreException.Reason.CRC_MISMATCH, ex.reason(),
                "被篡改的列块必须触发 CRC_MISMATCH，记录不得被静默读取");
    }

    @Test
    void corruptedManifestFailsFast(@TempDir Path dir) throws Exception {
        StoreConfig cfg = StoreConfig.defaults(dir);
        Files.createDirectories(dir);
        Files.writeString(ManifestStore.stagingSegmentFile(dir, 1L).getParent()
                .resolve("manifest.json"), "{ this is not valid json ");
        StoreException ex = assertThrows(StoreException.class, () -> DiskColumnarStore.open(cfg));
        System.out.println("[损坏清单] 原因=" + ex.reason() + " 信息=" + ex.getMessage());
        assertEquals(StoreException.Reason.BAD_FORMAT, ex.reason(),
                "清单损坏必须拒绝启动，不能静默重置导致数据不可解释");
    }
}
