/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;

/**
 * 表的Entry对象，内存存储模式，Entry封装整张表，而不是单个对象。
 * 所以column.name需要withKeyName，withIndexKeyname
 */
public class TextBackendEntry implements BackendEntry, Cloneable {

    public static final String VALUE_SPLITOR = "\u0002";

    private final HugeType type;
    //内容是什么？？，vertexID.应该是null，type对应table的类型
    private final Id id;
    private Id subId;
    //存储column实际数据，使用排序map实现前缀范围查询
    private NavigableMap<String, String> columns;

    public TextBackendEntry(HugeType type, Id id) {
        this.type = type;
        this.id = id;
        this.subId = null;
        this.resetColumns();
    }

    @Override
    public HugeType type() {
        return this.type;
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public Id subId() {
        return this.subId;
    }

    public void subId(Id subId) {
        this.subId = subId;
    }

    public Set<String> columnNames() {
        return this.columns.keySet();
    }

    /*
        write column
     */
    public void column(HugeKeys column, String value) {
        this.columns.put(column.string(), value);
    }

    /**
     *
     * @param column HugeKeys.name
     * @param value value
     */
    public void column(String column, String value) {
        this.columns.put(column, value);
    }

    /*
        select column
     */
    public String column(HugeKeys column) {
        return this.columns.get(column.string());
    }

    /**
     * select column
     * @param column column.name
     * @return column.value
     */
    public String column(String column) {
        return this.columns.get(column);
    }

    public BackendColumn columns(String column) {
        String value = this.columns.get(column);
        if (value == null) {
            return null;
        }
        return BackendColumn.of(StringEncoding.encode(column),
                                StringEncoding.encode(value));
    }

    public Collection<BackendColumn> columnsWithPrefix(String prefix) {
        return this.columnsWithPrefix(prefix, true, prefix);
    }

    /**
     * 返回start之后的，前缀为prefix的所有项
     * @param start
     * @param inclusiveStart
     * @param prefix
     * @return
     */
    public Collection<BackendColumn> columnsWithPrefix(String start,
                                                       boolean inclusiveStart,
                                                       String prefix) {
        List<BackendColumn> list = new ArrayList<>();
        Map<String, String> map = this.columns.tailMap(start, inclusiveStart);
        for (Map.Entry<String, String> e : map.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key.startsWith(prefix)) {
                list.add(BackendColumn.of(StringEncoding.encode(key),
                                          StringEncoding.encode(value)));
            }
        }
        return list;
    }

    /**
     * 返回start与end之间的子图
     * @param start
     * @param inclusiveStart
     * @param end
     * @param inclusiveEnd
     * @return
     */
    public Collection<BackendColumn> columnsWithRange(String start,
                                                      boolean inclusiveStart,
                                                      String end,
                                                      boolean inclusiveEnd) {
        List<BackendColumn> list = new ArrayList<>();
        Map<String, String> map = this.columns.subMap(start, inclusiveStart,
                                                      end, inclusiveEnd);
        for (Map.Entry<String, String> e : map.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            list.add(BackendColumn.of(StringEncoding.encode(key),
                                      StringEncoding.encode(value)));
        }
        return list;
    }

    /**
     * 是否包含key
     * @param column
     * @return
     */
    public boolean contains(String column) {
        return this.columns.containsKey(column);
    }

    /**
     * 是否包含key，并且value相同
     * @param column
     * @param value
     * @return
     */
    public boolean contains(String column, String value) {
        String col = this.columns.get(column);
        return col != null && col.equals(value);
    }

    /**
     * 是否包含前缀
     * @param column
     * @return
     */
    public boolean containsPrefix(String column) {
        Map<String, String> map = this.columns.tailMap(column, true);
        for (String c : map.keySet()) {
            if (c.startsWith(column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 是否包含value,需要遍历所有数据
     * @param value
     * @return
     */
    public boolean containsValue(String value) {
        return this.columns.values().contains(value);
    }

    /*
        update column
     */
    /**
     * 写入索引数据
     * 追加entry的columns到存储中，如果存在追加，如果不存在则新增
     * @param entry 输入
     */
    public void append(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());
            //Property 覆盖
            // TODO: use more general method
            if (col.getKey().startsWith(HugeType.PROPERTY.string())) {
                this.columns.put(col.getKey(), col.getValue());
                continue;
            }

            //Element_ids 值合并
            // TODO: use more general method
            if (!col.getKey().endsWith(HugeKeys.ELEMENT_IDS.string())) {
                continue;
            }

            // TODO: ensure the old value is a list and json format (for index)
            List<Object> values = new ArrayList<>();
            Object[] oldValues = JsonUtil.fromJson(oldValue, Object[].class);
            Object[] newValues = JsonUtil.fromJson(newValue, Object[].class);
            values.addAll(Arrays.asList(oldValues));
            values.addAll(Arrays.asList(newValues));
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    /**
     * 删除entry与存储中存在交际的数据，将剩下的column更新回存储
     * PROPERTY,EDGE_OUT,EDGE_IN key相同删除项目。
     * ELEMENT_IDS 存在多个值，删除重复的部分
     * @param entry 输入
     */
    public void eliminate(TextBackendEntry entry) {
        for (Entry<String, String> col : entry.columns.entrySet()) {
            String newValue = col.getValue();
            String oldValue = this.column(col.getKey());

            // TODO: use more general method
            if (col.getKey().startsWith(HugeType.PROPERTY.string()) ||
                col.getKey().startsWith(HugeType.EDGE_OUT.string()) ||
                col.getKey().startsWith(HugeType.EDGE_IN.string())) {
                this.columns.remove(col.getKey());
                continue;
            }

            // TODO: use more general method
            if (!col.getKey().endsWith(HugeKeys.ELEMENT_IDS.string())) {
                continue;
            }

            // TODO: ensure the old value is a list and json format (for index)
            List<Object> values = new ArrayList<>();
            Object[] oldValues = JsonUtil.fromJson(oldValue, Object[].class);
            Object[] newValues = JsonUtil.fromJson(newValue, Object[].class);
            values.addAll(Arrays.asList(oldValues));
            values.removeAll(Arrays.asList(newValues));
            // Update the old value
            this.column(col.getKey(), JsonUtil.toJson(values));
        }
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    @Override
    public int columnsSize() {
        return this.columns.size();
    }

    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> list = new ArrayList<>(this.columns.size());
        for (Entry<String, String> column : this.columns.entrySet()) {
            BackendColumn bytesColumn = new BackendColumn();
            bytesColumn.name = StringEncoding.encode(column.getKey());
            bytesColumn.value = StringEncoding.encode(column.getValue());
            list.add(bytesColumn);
        }
        return list;
    }

    /**
     * 写入columns - put
     * @param bytesColumns
     */
    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        for (BackendColumn column : bytesColumns) {
            this.columns.put(StringEncoding.decode(column.name),
                             StringEncoding.decode(column.value));
        }
    }

    @Override
    public void columns(BackendColumn... bytesColumns) {
        for (BackendColumn column : bytesColumns) {
            this.columns.put(StringEncoding.decode(column.name),
                             StringEncoding.decode(column.value));
        }
    }

    /**
     * 根据Map的Key进行覆盖
     * @param other
     */
    @Override
    public void merge(BackendEntry other) {
        TextBackendEntry text = (TextBackendEntry) other;
        this.columns.putAll(text.columns);
    }

    @Override
    public void clear() {
        this.columns.clear();
    }

    private void resetColumns() {
        this.columns = new ConcurrentSkipListMap<>();
    }

    /*
    硬copy
     */
    public TextBackendEntry copy() {
        try {
            //软拷贝
            TextBackendEntry clone = (TextBackendEntry) this.clone();
            //数据内容硬拷贝
            clone.columns = new ConcurrentSkipListMap<>(this.columns);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
    }

    /**
     * 拷贝后几个column
     * @param count column个数
     * @return
     */
    public TextBackendEntry copyLast(int count) {
        TextBackendEntry clone;
        try {
            clone = (TextBackendEntry) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
        clone.resetColumns();

        // Copy the last count columns
        Iterator<Entry<String, String>> it = this.columns.entrySet().iterator();
        final int skip = this.columns.size() - count;
        for (int i = 0; it.hasNext(); i++) {
            Entry<String, String> entry = it.next();
            if (i < skip) {
                continue;
            }
            clone.columns.put(entry.getKey(), entry.getValue());
        }
        return clone;
    }

    /**
     * 拷贝头
     * @param count
     * @return
     */
    public TextBackendEntry copyHead(int count) {
        TextBackendEntry clone;
        try {
            clone = (TextBackendEntry) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
        clone.resetColumns();

        // Copy the head count columns
        Iterator<Entry<String, String>> it = this.columns.entrySet().iterator();
        for (int i = 0; it.hasNext() && i < count; i++) {
            Entry<String, String> entry = it.next();
            clone.columns.put(entry.getKey(), entry.getValue());
        }
        return clone;
    }

    /**
     * 比较id，column 是否相等
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextBackendEntry)) {
            return false;
        }
        TextBackendEntry other = (TextBackendEntry) obj;
        if (this.id() != other.id() && !this.id().equals(other.id())) {
            return false;
        }
        if (this.columns().size() != other.columns().size()) {
            return false;
        }
        for (Map.Entry<String, String> e : this.columns.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            String otherValue = other.columns.get(key);
            if (otherValue == null) {
                return false;
            }
            if (!value.equals(otherValue)) {
                return false;
            }
        }
        return true;
    }
}
