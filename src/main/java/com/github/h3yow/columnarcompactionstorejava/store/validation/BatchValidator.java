package com.github.h3yow.columnarcompactionstorejava.store.validation;

import com.github.h3yow.columnarcompactionstorejava.store.model.BatchRejectReason;
import com.github.h3yow.columnarcompactionstorejava.store.model.BatchRejection;
import com.github.h3yow.columnarcompactionstorejava.store.model.ColumnDef;
import com.github.h3yow.columnarcompactionstorejava.store.model.DataType;
import com.github.h3yow.columnarcompactionstorejava.store.model.Row;
import com.github.h3yow.columnarcompactionstorejava.store.model.Schema;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 批次校验器。所有不合法的批次都会得到一个带明确原因的 {@link BatchRejection}，
 * 绝不静默丢弃任何行或字段。
 *
 * <p>拒绝原因与判定依据：
 * <ul>
 *   <li>{@code MALFORMED_BATCH}：batchId 空、列为空/重名/null、行集合为空、行为 null；</li>
 *   <li>{@code MISSING_FIELD}：某行缺少列头中声明的字段（每个批次都必须自包含，
 *       不允许「省略」字段，省略只能显式写 null）；</li>
 *   <li>{@code UNKNOWN_FIELD}：某行出现列头未声明的字段；</li>
 *   <li>{@code TYPE_MISMATCH}：值的 Java 类型与列类型不兼容（Long/Double/Boolean/String 严格匹配）；</li>
 *   <li>{@code DUPLICATE_BATCH_ID}：batchId 已在此前成功提交的段中；</li>
 *   <li>{@code BATCH_TOO_LARGE}：估算字节数超过单批次内存上限。</li>
 * </ul>
 *
 * <p>新增列：列头中出现、当前 schema 中不存在的名字按「新列」处理，其类型由本批次首个
 * 非 null 值确定（全部为 null 无法定型 → MALFORMED_BATCH）。新列在写成功后以追加方式
 * 进入 schema，旧数据对该列恒为 null（由读取端按 schema 版本对齐）。
 */
public final class BatchValidator {

    private BatchValidator() {
    }

    /** 校验通过后的解析结果：本批次引用的列（按列头顺序）、新列与列值列式投影。 */
    public static final class Resolved {
        private final List<ColumnDef> columns;
        private final List<ColumnDef> newColumns;
        private final List<List<Object>> columnValues;
        private final long estimatedBytes;

        Resolved(List<ColumnDef> columns, List<ColumnDef> newColumns,
                 List<List<Object>> columnValues, long estimatedBytes) {
            this.columns = columns;
            this.newColumns = newColumns;
            this.columnValues = columnValues;
            this.estimatedBytes = estimatedBytes;
        }

        public List<ColumnDef> columns() {
            return columns;
        }

        public List<ColumnDef> newColumns() {
            return newColumns;
        }

        public List<List<Object>> columnValues() {
            return columnValues;
        }

        public long estimatedBytes() {
            return estimatedBytes;
        }
    }

    /**
     * 校验并解析批次。
     *
     * @param knownBatchIds 已成功提交的 batchId 集合
     * @param maxBatchBytes 单批次内存字节上限；&lt;=0 表示不检查
     * @return 校验通过返回 Resolved；否则返回带原因的拒绝
     */
    public static Object validate(WriteBatch batch, Schema schema,
                                  Set<String> knownBatchIds, long maxBatchBytes) {
        BatchRejection rejection = checkStructure(batch);
        if (rejection != null) {
            return rejection;
        }

        // 1. 解析列头：既有列按名字解析（类型固定），新名字按新列处理
        Set<String> headerSet = new HashSet<>();
        List<ColumnDef> resolvedColumns = new ArrayList<>(batch.columns().size());
        List<ColumnDef> newColumns = new ArrayList<>();
        int nextNewId = schema.version();
        for (String name : batch.columns()) {
            if (!headerSet.add(name)) {
                return reject(BatchRejectReason.MALFORMED_BATCH, name,
                        "列头中列名重复: " + name);
            }
            ColumnDef existing = schema.findByName(name).orElse(null);
            if (existing != null) {
                resolvedColumns.add(existing);
            } else {
                DataType inferred;
                Object firstNonNull = null;
                for (Row rr : batch.rows()) {
                    Object vv = rr.values().get(name);
                    if (vv != null) {
                        firstNonNull = vv;
                        break;
                    }
                }
                if (firstNonNull == null) {
                    return reject(BatchRejectReason.MALFORMED_BATCH, name,
                            "新列 '" + name + "' 在本批次所有值均为 null，无法确定类型；"
                                    + "请在至少一行提供非 null 值以声明类型");
                }
                inferred = dataTypeOf(firstNonNull);
                if (inferred == null) {
                    // 首个值类型不受支持：明确按类型不匹配拒绝，而不是静默猜测
                    return reject(BatchRejectReason.TYPE_MISMATCH, name,
                            "新列 '" + name + "' 的首个值类型不受支持: "
                                    + firstNonNull.getClass().getSimpleName());
                }
                ColumnDef nc = new ColumnDef(nextNewId++, name, inferred);
                newColumns.add(nc);
                resolvedColumns.add(nc);
            }
        }

        // 2. 逐行检查：缺字段 / 多字段 / 类型不匹配（任何一行失败即拒绝整批，保证原子性）
        Map<String, ColumnDef> byName = new LinkedHashMap<>();
        for (ColumnDef c : resolvedColumns) {
            byName.put(c.name(), c);
        }
        int rowCount = batch.rows().size();
        for (int r = 0; r < rowCount; r++) {
            Row row = batch.rows().get(r);
            Map<String, Object> values = row.values();
            for (String name : batch.columns()) {
                if (!values.containsKey(name)) {
                    return reject(BatchRejectReason.MISSING_FIELD, name,
                            "第 " + r + " 行缺少列头声明的字段 '" + name
                                    + "'（不允许隐式省略，缺失请显式提供 null）");
                }
            }
            for (String key : values.keySet()) {
                if (!byName.containsKey(key)) {
                    return reject(BatchRejectReason.UNKNOWN_FIELD, key,
                            "第 " + r + " 行出现列头未声明的字段 '" + key + "'");
                }
            }
            for (ColumnDef c : resolvedColumns) {
                Object v = values.get(c.name());
                if (v != null && !isCompatible(v, c.type())) {
                    return reject(BatchRejectReason.TYPE_MISMATCH, c.name(),
                            "第 " + r + " 行列 '" + c.name() + "' 期望 " + c.type()
                                    + "，实际值 " + describe(v) + " (" + v.getClass().getSimpleName() + ")");
                }
            }
        }

        // 3. 幂等去重
        if (knownBatchIds.contains(batch.batchId())) {
            return reject(BatchRejectReason.DUPLICATE_BATCH_ID, null,
                    "batchId '" + batch.batchId() + "' 已成功写入，重复提交被拒绝");
        }

        // 4. 列式投影 + 估算字节，检查内存上限
        List<List<Object>> columnValues = new ArrayList<>(resolvedColumns.size());
        long estimated = 0L;
        for (ColumnDef c : resolvedColumns) {
            List<Object> vals = new ArrayList<>(rowCount);
            for (Row row : batch.rows()) {
                Object v = row.values().get(c.name());
                vals.add(v);
                estimated += estimateValueBytes(c.type(), v);
            }
            columnValues.add(vals);
        }
        if (maxBatchBytes > 0 && estimated > maxBatchBytes) {
            return reject(BatchRejectReason.BATCH_TOO_LARGE, null,
                    "批次估算 " + estimated + " 字节，超过上限 " + maxBatchBytes + " 字节");
        }

        return new Resolved(resolvedColumns, newColumns, columnValues, estimated);
    }

    private static BatchRejection reject(BatchRejectReason reason, String column, String detail) {
        return new BatchRejection(reason, column, detail);
    }

    private static BatchRejection checkStructure(WriteBatch batch) {
        if (batch == null) {
            return reject(BatchRejectReason.MALFORMED_BATCH, null, "批次为 null");
        }
        if (batch.batchId() == null || batch.batchId().isBlank()) {
            return reject(BatchRejectReason.MALFORMED_BATCH, null, "batchId 为空");
        }
        if (batch.columns() == null || batch.columns().isEmpty()) {
            return reject(BatchRejectReason.MALFORMED_BATCH, null,
                    "批次列头为空，缺少格式信息");
        }
        for (String c : batch.columns()) {
            if (c == null || c.isBlank()) {
                return reject(BatchRejectReason.MALFORMED_BATCH, null, "列头包含空列名");
            }
        }
        if (batch.rows() == null || batch.rows().isEmpty()) {
            return reject(BatchRejectReason.MALFORMED_BATCH, null,
                    "批次不包含任何行（空批次不允许写入）");
        }
        for (int i = 0; i < batch.rows().size(); i++) {
            Row row = batch.rows().get(i);
            if (row == null || row.values() == null) {
                return reject(BatchRejectReason.MALFORMED_BATCH, null,
                        "第 " + i + " 行为 null 或 values 为 null");
            }
        }
        return null;
    }

    private static DataType dataTypeOf(Object v) {
        if (v instanceof Long) {
            return DataType.LONG;
        }
        if (v instanceof Double) {
            return DataType.DOUBLE;
        }
        if (v instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (v instanceof String) {
            return DataType.STRING;
        }
        return null;
    }

    private static boolean isCompatible(Object v, DataType type) {
        return switch (type) {
            case LONG -> v instanceof Long;
            case DOUBLE -> v instanceof Double;
            case BOOLEAN -> v instanceof Boolean;
            case STRING -> v instanceof String;
        };
    }

    private static String describe(Object v) {
        String s = String.valueOf(v);
        return s.length() <= 40 ? s : s.substring(0, 40) + "...";
    }

    /** 估算单个值在列式缓冲中的字节占用（null 仅占 1 字节标志位）。 */
    public static long estimateValueBytes(DataType type, Object v) {
        if (v == null) {
            return 1L;
        }
        return switch (type) {
            case LONG, DOUBLE -> 1L + 8L;
            case BOOLEAN -> 1L + 1L;
            case STRING -> 1L + 4L + ((String) v).getBytes(StandardCharsets.UTF_8).length;
        };
    }
}
