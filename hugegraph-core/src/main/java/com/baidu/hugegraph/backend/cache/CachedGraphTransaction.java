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

package com.baidu.hugegraph.backend.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableSet;

public final class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;
    //根据顶点id缓存
    private final Cache verticesCache;
    //根据查询（查询对象）缓存数据
    private final Cache edgesCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedGraphTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        HugeConfig conf = graph.configuration();

        int capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", capacity, expire);

        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", capacity, expire);

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.unlistenChanges();
        }
    }

    private Cache cache(String prefix, int capacity, long expire) {
        String name = prefix + "-" + super.graph().name();
        Cache cache = CacheManager.instance().cache(name, capacity);
        cache.expire(expire);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                          this.graph(), event.name());
                this.verticesCache.clear();
                this.edgesCache.clear();
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                      this.graph(), event);
            event.checkArgs(String.class, Id.class);
            Object[] args = event.args();
            if ("invalid".equals(args[0])) {
                Id id = (Id) args[1];
                if (this.verticesCache.get(id) != null) {
                    // Invalidate vertex cache
                    this.verticesCache.invalidate(id);
                } else if (this.edgesCache.get(id) != null) {
                    // Invalidate edge cache
                    this.edgesCache.invalidate(id);
                }
                return true;
            } else if ("clear".equals(args[0])) {
                this.verticesCache.clear();
                this.edgesCache.clear();
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.graph().graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.graph().graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    /**
     * 查询vertex数据,先缓存在底层
     * 针对定点查询有优化 (IdQuery) query
     * 正常查询 queryVerticesFromBackend
     * @param query 查询封装
     * @return
     */
    @Override
    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            //只通过id进行查询
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    /**
     * 根据vertex id查询，先查询缓存，再查询底层
     * @param query
     * @return
     */
    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        //用于保存缓存中不存在的ids
        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            //通过id可以利用缓存
            Object vertex = this.verticesCache.get(vertexId);
            if (vertex != null) {
                vertices.add((HugeVertex) vertex);
            } else {
                //缓存中没有找到结果，保存为找到结果的vertexId
                newQuery.query(vertexId);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<HugeVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            //缓存中一条没有，则完全由底层查询
            newQuery = query;
        }

        if (!newQuery.empty()) {
            //对缓存中不存在的重新查询底层
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            //更新这部分缓存
            // Generally there are not too much data with id query
            ListIterator<HugeVertex> listIterator = QueryResults.toList(rs);
            for (HugeVertex vertex : listIterator.list()) {
                this.verticesCache.update(vertex.id(), vertex);
            }
            results.extend(listIterator);
        }

        return results;
    }

    /**
     * 根据query查询对象，查询边
     * 先获取缓存（将查询封装成ID进行缓存与查询），如果不存在则查询底层，
     * 如果数据量小于MAX_CACHE_EDGES_PER_QUERY，则进行缓存
     * @param query
     * @return
     */
    @Override
    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        if (query.empty() || query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            //大数据量
            return super.queryEdgesFromBackend(query);
        }
        //将查询对象封装成ID
        //edge也是通过id进行查询,这个id是????
        //一个id对应多个edge
        //每个查询缓存限制
        Id cacheKey = new QueryId(query);
        Object value = this.edgesCache.get(cacheKey);
        if (value != null) {
            @SuppressWarnings("unchecked")
            Collection<HugeEdge> edges = (Collection<HugeEdge>) value;
            return edges.iterator();
        } else {
            Iterator<HugeEdge> rs = super.queryEdgesFromBackend(query);
            /*
             * Iterator can't be cached, caching list instead
             * Generally there are not too much data with id query
             */
            ListIterator<HugeEdge> listIterator = QueryResults.toList(rs);
            Collection<HugeEdge> edges = listIterator.list();
            if (edges.size() == 0) {
                this.edgesCache.update(cacheKey, Collections.emptyList());
            } else if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
                this.edgesCache.update(cacheKey, edges);
            }
            return listIterator;
        }
    }

    /**
     * 同步缓存
     * 将顶点的新增，修改，删除同步到缓存
     * 当前策略，如果edge发生修改则清空edgesCache
     * @param mutations
     */
    @Override
    protected void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        //等待提交的顶点数据（新增，修改，删除）
        Collection<HugeVertex> changes = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        //等待提交的边数据（新增，修改，删除）
        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commitMutation2Backend(mutations);
            // Update vertex cache
            for (HugeVertex vertex : changes) {
                //提交结束后关闭事务，缓存中的数据不携带Tx
                vertex = vertex.resetTx();
                this.verticesCache.updateIfPresent(vertex.id(), vertex);
            }
        } finally {
            // Update removed vertex in cache whatever success or fail
            for (HugeVertex vertex : deletions) {
                this.verticesCache.invalidate(vertex.id());
            }

            // Update edge cache if any edges change
            if (edgesInTxSize > 0) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
            }
        }
    }

    /**
     * 删除索引操作，更新edge缓存
     * @param indexLabel
     */
    @Override
    public void removeIndex(IndexLabel indexLabel) {
        try {
            super.removeIndex(indexLabel);
        } finally {
            //因为vertex是根据id缓存的，所以不受影响
            // Update edge cache if needed (any edge-index is deleted)
            //如果edge-index被删除，则需要更新edge cache，删除对应的query。
            //因为edge cache是根据query缓存的，edge-index被删除则对应的query就不存在了
            if (indexLabel.baseType() == HugeType.EDGE_LABEL) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
            }
        }
    }
}
