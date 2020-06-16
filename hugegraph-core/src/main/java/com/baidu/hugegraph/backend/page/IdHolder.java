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

package com.baidu.hugegraph.backend.page;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

/**
 * 索引查询封装类，查询索引，返回相关结果elementIds
 * PageIds fetchNext(String page, long pageSize)
 * 支持分页模式PagingIdHolder
 * 联合索引，不支持分页FixedIdHolder
 */
public abstract class IdHolder {

    protected final Query query;
    //分页数据是否枯竭
    protected boolean exhausted;

    public IdHolder(Query query) {
        E.checkNotNull(query, "query");;
        this.query = query;
        this.exhausted = false;
    }

    public Query query() {
        return this.query;
    }

    @Override
    public String toString() {
        return String.format("%s{origin:%s,final:%s}",
                             this.getClass().getSimpleName(),
                             this.query.originQuery(), this.query);
    }
    //是否为分页模式
    public abstract boolean paging();
    //返回所有结果ids
    public abstract Set<Id> all();
    //获取下一页结果
    public abstract PageIds fetchNext(String page, long pageSize);

    /**
     * 用于联合索引，不支持分页
     */
    public static class FixedIdHolder extends IdHolder {

        // Used by Joint Index 联合索引
        private final Set<Id> ids;

        public FixedIdHolder(Query query, Set<Id> ids) {
            super(query);
            this.ids = ids;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return this.ids;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("FixedIdHolder.fetchNext");
        }
    }

    /**
     * 索引查询分页模式
     */
    public static class PagingIdHolder extends IdHolder {
        //索引查询，根据ConditionQuery返回命中elementIds
        private final Function<ConditionQuery, PageIds> fetcher;

        public PagingIdHolder(ConditionQuery query,
                              Function<ConditionQuery, PageIds> fetcher) {
            super(query.copy());
            E.checkArgument(query.paging(),
                            "Query '%s' must include page info", query);
            this.fetcher = fetcher;
        }

        @Override
        public boolean paging() {
            return true;
        }

        /**
         * 根据分页信息返回PageIds
         * @param page
         * @param pageSize
         * @return
         */
        @Override
        public PageIds fetchNext(String page, long pageSize) {
            if (this.exhausted) {
                return PageIds.EMPTY;
            }

            this.query.page(page);
            this.query.limit(pageSize);

            PageIds result = this.fetcher.apply((ConditionQuery) this.query);
            assert result != null;
            if (result.ids().size() < pageSize || result.page() == null) {
                this.exhausted = true;
            }
            return result;
        }

        @Override
        public Set<Id> all() {
            throw new NotImplementedException("PagingIdHolder.all");
        }
    }

    /**
     * 索引批量模式，不支持分页模式
     * 一次索引查询所有数据，每次获取并反序列化batchSize数量的数据
     * 最后返回pageIds
     */
    public static class BatchIdHolder extends IdHolder
                                      implements CIter<IdHolder> {
        //索引底层查询结果Iterator，query执行结果
        private final Iterator<BackendEntry> entries;
        //根据batchSize遍历entries的执行函数，返回指定数量的结果
        private final Function<Long, Set<Id>> fetcher;
        private long count;     //已返回的数据总量

        public BatchIdHolder(ConditionQuery query,
                             Iterator<BackendEntry> entries,
                             Function<Long, Set<Id>> fetcher) {
            super(query);
            this.entries = entries;
            this.fetcher = fetcher;
            this.count = 0L;
        }

        @Override
        public boolean paging() {
            return false;
        }

        /**
         * 是否还有数据未处理
         * @return
         */
        @Override
        public boolean hasNext() {
            if (this.exhausted) {
                return false;
            }
            boolean hasNext = this.entries.hasNext();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
        }

        @Override
        public IdHolder next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return this;
        }

        /**
         * 遍历entries，返回batchSize数量的PageIds
         * @param page null
         * @param batchSize 批次大小
         * @return
         */
        @Override
        public PageIds fetchNext(String page, long batchSize) {
            E.checkArgument(page == null,
                            "Not support page parameter by BatchIdHolder");
            E.checkArgument(batchSize >= 0L,
                            "Invalid batch size value: %s", batchSize);

            if (!this.query.nolimit()) {
                long remaining = this.remaining();
                if (remaining < batchSize) {
                    batchSize = remaining;
                }
            }
            assert batchSize >= 0L : batchSize;
            Set<Id> ids = this.fetcher.apply(batchSize);
            int size = ids.size();
            this.count += size;
            //已经没有剩余数据了
            if (size < batchSize || size == 0) {
                this.close();
            }

            // If there is no data, the entries is not a Metadatable object
            if (size == 0) {
                return PageIds.EMPTY;
            } else {
                return new PageIds(ids, PageState.EMPTY);
            }
        }

        /**
         * 返回所有数据
         * @return
         */
        @Override
        public Set<Id> all() {
            Set<Id> ids = this.fetcher.apply(this.remaining());
            this.count += ids.size();
            this.close();
            return ids;
        }

        /**
         * 剩余的数据量
         * @return
         */
        private long remaining() {
            if (this.query.nolimit()) {
                //返回Long最大数量
                return Query.NO_LIMIT;
            } else {
                //offset+limit 等于查询的所有数据
                //offset+limit-count 剩余的数量
                return this.query.total() - this.count;
            }
        }

        /**
         * 关闭entries迭代器
         */
        @Override
        public void close() {
            if (this.exhausted) {
                return;
            }
            this.exhausted = true;

            CloseableIterator.closeIterator(this.entries);
        }

        @Override
        public Object metadata(String meta, Object... args) {
            E.checkState(this.entries instanceof Metadatable,
                         "Invalid iterator for Metadatable: %s",
                         this.entries.getClass());
            return ((Metadatable) this.entries).metadata(meta, args);
        }
    }
}
