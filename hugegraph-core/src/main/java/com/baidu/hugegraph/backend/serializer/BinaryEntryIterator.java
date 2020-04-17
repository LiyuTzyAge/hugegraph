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

import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.util.E;

public class BinaryEntryIterator<Elem> extends BackendEntryIterator {
    //查询结果 Elem 结果类型:hbase=Result,等
    protected final BackendIterator<Elem> results;
    //解析结果并与上次结果进行合并,因hbase多个column在不同Row中
    // 函数 BackendEntry：上次结果、Elem 查询记过、BackendEntry 返回结果
    protected final BiFunction<BackendEntry, Elem, BackendEntry> merger;
    //对象缓存，
    protected BackendEntry next;

    /**
     * 查询结果解析迭代器
     * @param results 查询结果
     * @param query 查询语句
     * @param m 查询结果解析函数
     */
    public BinaryEntryIterator(BackendIterator<Elem> results, Query query,
                               BiFunction<BackendEntry, Elem, BackendEntry> m) {
        super(query);

        E.checkNotNull(results, "results");
        E.checkNotNull(m, "merger");

        this.results = results;
        this.merger = m;
        this.next = null;

        this.skipOffset();

        if (query.paging()) {
            this.skipPageOffset(query.page());
        }
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        this.results.close();
    }

    /**
     * 解析，获取查询结果中的内容，将当前结果写入current，如果存在多个结果，
     * 下次写入到current中
     * @return 是否读取到数据
     */
    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }
        //遍历结果 直到达到上限INLINE_BATCH_SIZE
        while (this.results.hasNext()) {
            Elem elem = this.results.next();
            /*
            解析结果，合并到current中
             */
            BackendEntry merged = this.merger.apply(this.current, elem);
            E.checkState(merged != null, "Error when merging entry");
            if (this.current == null) {
                //first ->current=null and next=null
                // The first time to read
                this.current = merged;
            } else if (merged == this.current) {
                //遍历同一对象（RowKey）的不同字段
                // The next entry belongs to the current entry
                assert this.current != null;
                if (this.sizeOf(this.current) >= INLINE_BATCH_SIZE) {
                    break;
                }
            } else {
                //如果 遍历过程中，遇到了新的元素，则缓存到next中，break
                // New entry
                assert this.next == null;
                this.next = merged;
                break;
            }

            //是否到达query中limit的上限
            // When limit exceed, stop fetching
            if (this.reachLimit(this.fetched() - 1)) {
                // Need remove last one because fetched limit + 1 records
                this.removeLastRecord();
                this.results.close();   //结果解析全部完成
                break;
            }
        }

        return this.current != null;
    }

    /**
     * 结果字段数
     * @param entry
     * @return
     */
    @Override
    protected final long sizeOf(BackendEntry entry) {
        /*
         * One edge per column (one entry <==> a vertex),
         * or one element id per column (one entry <==> an index)
         */
        if (entry.type().isEdge() || entry.type().isIndex()) {
            return entry.columnsSize();
        }
        return 1L;
    }

    /**
     * 跳过多个column
     * @param entry
     * @param skip 字段个数
     * @return
     */
    @Override
    protected final long skip(BackendEntry entry, long skip) {
        BinaryBackendEntry e = (BinaryBackendEntry) entry;
        E.checkState(e.columnsSize() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.removeColumn(0);
        }
        return e.columnsSize();
    }

    @Override
    protected PageState pageState() {
        byte[] position = this.results.position();
        if (position == null) {
            position = PageState.EMPTY_BYTES;
        }
        return new PageState(position, 0, (int) this.count());
    }

    /**
     * 删除最后一个字段
     */
    private void removeLastRecord() {
        int lastOne = this.current.columnsSize() - 1;
        ((BinaryBackendEntry) this.current).removeColumn(lastOne);
    }

    private void skipPageOffset(String page) {
        PageState pagestate = PageState.fromString(page);
        if (pagestate.offset() > 0 && this.fetch()) {
            this.skip(this.current, pagestate.offset());
        }
    }
}
