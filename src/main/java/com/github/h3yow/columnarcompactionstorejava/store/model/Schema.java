package com.github.h3yow.columnarcompactionstorejava.store.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 不可变 schema：列按 id 升序排列。
 *
 * <p>兼容规则（关键）：
 * <ul>
 *   <li>列 id 密集且稳定：新增列只能以「追加」方式获得 id = 当前列数，旧列 id 永不复用；</li>
 *   <li>段文件记录写入时的 schema 版本（= 当时列数），读取时按 id 对齐，
 *       旧段中 id &gt;= 段版本的列一律解释为 null —— 新字段对旧数据的缺省语义恒为 null；</li>
 *   <li>列改名 / 删列不被支持：写入端若以新名字出现，只会被当作「新增列」追加，
 *       旧名字仍然存在，因此读取端绝不会发生按位置静默错列。</li>
 * </ul>
 */
public final class Schema {

    private final List<ColumnDef> columns;
    private final Map<String, ColumnDef> byName;
    private final Map<Integer, ColumnDef> byId;

    public Schema(List<ColumnDef> columns) {
        Objects.requireNonNull(columns, "columns");
        List<ColumnDef> copy = new ArrayList<>(columns.size());
        Map<String, ColumnDef> names = new HashMap<>();
        Map<Integer, ColumnDef> ids = new HashMap<>();
        int expectedId = 0;
        for (ColumnDef c : columns) {
            Objects.requireNonNull(c, "column");
            Objects.requireNonNull(c.name(), "column name");
            Objects.requireNonNull(c.type(), "column type");
            // id 必须密集递增，保证「id 即稳定槽位」且缺省语义可按版本推导
            if (c.id() != expectedId) {
                throw new IllegalArgumentException(
                        "列 id 必须密集递增，期望 " + expectedId + " 实际 " + c.id());
            }
            if (names.put(c.name(), c) != null) {
                throw new IllegalArgumentException("列名重复: " + c.name());
            }
            ids.put(c.id(), c);
            copy.add(c);
            expectedId++;
        }
        this.columns = Collections.unmodifiableList(copy);
        this.byName = Collections.unmodifiableMap(names);
        this.byId = Collections.unmodifiableMap(ids);
    }

    public static Schema empty() {
        return new Schema(List.of());
    }

    public List<ColumnDef> columns() {
        return columns;
    }

    public Optional<ColumnDef> findByName(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    public ColumnDef byId(int id) {
        ColumnDef c = byId.get(id);
        if (c == null) {
            throw new IllegalArgumentException("未知列 id: " + id);
        }
        return c;
    }

    /** schema 版本：当前列数。段写入时记录，读取时据此判断旧段缺失哪些新列。 */
    public int version() {
        return columns.size();
    }

    /** 追加新列；新列 id 必须恰好从当前列数开始连续分配。 */
    public Schema withAppendedColumns(List<ColumnDef> extra) {
        Objects.requireNonNull(extra, "extra");
        List<ColumnDef> all = new ArrayList<>(columns);
        all.addAll(extra);
        return new Schema(all);
    }
}
