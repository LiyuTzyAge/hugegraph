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

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.util.E;

/**
 * 分页查询结果封装实现
 * 此类不直接查询，返回结果时才执行查询
 */
public class PageEntryIterator implements CIter<BackendEntry> {

    private final QueryList queries;
    private final long pageSize;
    private final PageInfo pageInfo;
    //查询结果对象，会更新当前query对象
    private final QueryResults queryResults; // for upper layer
    //查询结果对象，缓存page查询的结果，切换page也会一同更新
    private QueryList.PageResults pageResults;
    private long remaining; //当前查询剩余需要读取数据量

    public PageEntryIterator(QueryList queries, long pageSize) {
        this.queries = queries;
        this.pageSize = pageSize;
        this.pageInfo = this.parsePageInfo();
        //不执行查询,封装当前PageEntryIterator，调用next，hasNext返回结果
        this.queryResults = new QueryResults(this);
        //页面结果 Empty
        this.pageResults = QueryList.PageResults.EMPTY;
        //查询条件中的limit
        this.remaining = queries.parent().limit();
    }

    /**
     * 根据query条件，解析pageInfo
     * @return
     */
    private PageInfo parsePageInfo() {
        String page = this.queries.parent().pageWithoutCheck();
        PageInfo pageInfo = PageInfo.fromString(page);
        E.checkState(pageInfo.offset() < this.queries.total(),
                     "Invalid page '%s' with an offset '%s' exceeds " +
                     "the size of IdHolderList", page, pageInfo.offset());
        return pageInfo;
    }

    /**
     * 是否还有新数据，如果当前page读取完成，将会fetch新数据
     * 直到达到limit
     * @return
     */
    @Override
    public boolean hasNext() {
        if (this.pageResults.get().hasNext()) {
            return true;
        }
        return this.fetch();
    }

    /**
     * 执行底层数据查询，如果有新数据则返回true，否则false。直到读取足够的数据量
     * 查询过程中会更新pageInfo信息，目的滚动pageInfo。
     * 先滚动page.position，如果没有新数据会滚动subQuery
     * @return
     */
    private boolean fetch() {
        //越界，remaining<=0 读取数据量已经足够
        if ((this.remaining != Query.NO_LIMIT && this.remaining <= 0L) ||
            this.pageInfo.offset() >= this.queries.total()) {
            return false;
        }
        //实际数据量小于pageSize
        long pageSize = this.pageSize;
        if (this.remaining != Query.NO_LIMIT && this.remaining < pageSize) {
            pageSize = this.remaining;
        }
        this.closePageResults();    //因为pageResults.hasNext=false
        //QueryList 底层查询返回结果，返回pageSize数量的结果
        this.pageResults = this.queries.fetchNext(this.pageInfo, pageSize);
        assert this.pageResults != null;
        //设置查询
        this.queryResults.setQuery(this.pageResults.query());
        /*
        滚动pageInfo更新pageInfo状态
        判断pageResults是否存在未读数据
        if 无数据：
            increase() 切换subQuery，重置position
        if 有数据:
            更新pageInfo信息：
                if 是最后一个page，则切换subQuery，更新pageInfo的offset
                if 存在下一个page，则切换page索引，更新pageInfo的position
         */
        if (this.pageResults.get().hasNext()) {
            //更新pageInfo信息，等于滚动page
            if (!this.pageResults.hasNextPage()) {
                //为最后的页面，page数量+1,切换subQuery
                this.pageInfo.increase();
            } else {
                //存在下一个page，更新page信息，position
                this.pageInfo.page(this.pageResults.page());
            }
            //更新待查询剩余数据量，减去结果数据量
            this.remaining -= this.pageResults.total();
            //更新了pageResults，获取数据成功
            return true;
        } else {
            //pageResults无可读数据，切换写一个subQuery
            this.pageInfo.increase();
            //重新fetch数据
            return this.fetch();
        }
    }

    /**
     * 关闭结果迭代器
     */
    private void closePageResults() {
        if (this.pageResults != QueryList.PageResults.EMPTY) {
            CloseableIterator.closeIterator(this.pageResults.get());
        }
    }

    /**
     * 返回结果，下一个BackendEntry
     * @return
     */
    @Override
    public BackendEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.pageResults.get().next();
    }

    /**
     * 返回当前pageInfo信息
     * @param meta
     * @param args
     * @return
     */
    @Override
    public Object metadata(String meta, Object... args) {
        if (PageInfo.PAGE.equals(meta)) {
            if (this.pageInfo.offset() >= this.queries.total()) {
                return null;
            }
            return this.pageInfo;
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }

    @Override
    public void close() throws Exception {
        this.closePageResults();
    }

    public QueryResults results() {
        return this.queryResults;
    }
}
