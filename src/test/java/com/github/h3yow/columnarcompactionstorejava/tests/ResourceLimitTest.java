package com.github.h3yow.columnarcompactionstorejava.tests;

import com.github.h3yow.columnarcompactionstorejava.store.DiskColumnarStore;
import com.github.h3yow.columnarcompactionstorejava.store.config.StoreConfig;
import com.github.h3yow.columnarcompactionstorejava.store.exception.StoreException;
import com.github.h3yow.columnarcompactionstorejava.store.limiter.ResourceLimiter;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 资源约束：打开文件数有硬上限，超限操作等待，超时以可区分原因 RESOURCE_TIMEOUT 拒绝；
 * 许可释放后操作恢复成功，不发生许可泄漏。
 */
class ResourceLimitTest {

    private static Row row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        return new Row(m);
    }

    @Test
    void openFileLimitRejectsWithDistinctReasonThenRecovers(@TempDir Path dir) {
        // 上限 2、等待 300ms
        StoreConfig cfg = StoreConfig.defaults(dir)
                .withMaxOpenFiles(2)
                .withFileAcquireTimeoutMillis(300)
                .withCompactionThreshold(10);
        DiskColumnarStore store = DiskColumnarStore.open(cfg);
        for (int i = 0; i < 3; i++) {
            assertTrue(store.write(new WriteBatch("r" + i, List.of("id"),
                    List.of(row(i)))).accepted());
        }

        ResourceLimiter limiter = store.resourceLimiter();
        // 占满全部 2 个许可
        try (ResourceLimiter.Permit p1 = limiter.acquire();
             ResourceLimiter.Permit p2 = limiter.acquire()) {
            assertEquals(0, limiter.availablePermits());
            long t0 = System.currentTimeMillis();
            StoreException ex = null;
            try {
                store.scan(); // 需要打开段文件，必然等待后超时
            } catch (StoreException e) {
                ex = e;
            }
            long waited = System.currentTimeMillis() - t0;
            System.out.println("[文件上限] 拒绝原因=" + (ex == null ? null : ex.reason())
                    + " 等待约=" + waited + "ms 依据=" + (ex == null ? null : ex.getMessage()));
            assertTrue(ex != null, "超限必须被拒绝而不是无限等待");
            assertEquals(StoreException.Reason.RESOURCE_TIMEOUT, ex.reason(),
                    "超限拒绝原因必须与 IO/校验错误可区分");
            assertTrue(waited >= 250, "应至少等待到超时窗口");
        }

        // 许可释放后读取立即恢复，证明无许可泄漏
        assertEquals(3, store.scan().rows().size());
        System.out.println("[文件上限] 许可释放后读取恢复，许可剩余=" + limiter.availablePermits());
        assertEquals(2, limiter.availablePermits());
        store.close();
    }
}
