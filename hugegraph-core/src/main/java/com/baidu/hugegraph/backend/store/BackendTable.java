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

package com.baidu.hugegraph.backend.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;

public abstract class BackendTable<Session extends BackendSession, Entry> {

    private final String table;

    private final MetaDispatcher<Session> dispatcher;

    public BackendTable(String table) {
        this.table = table.toLowerCase();
        this.dispatcher = new MetaDispatcher<>();

        this.registerMetaHandlers();
    }

    public String table() {
        return this.table;
    }

    public MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    protected void registerMetaHandlers() {
        // pass
    }

    /**
     *  Mapping query-type to table-type
     *  查询类型映射成表类型，因Edge不同方向有两张表
     * @param query origin query
     * @return corresponding table type
     */
    public static HugeType tableType(Query query) {
        //type is vertex or edge
        HugeType type = query.resultType();

        // Mapping EDGE to EDGE_OUT/EDGE_IN
        if (type == HugeType.EDGE) {
            // We assume query OUT edges
            type = HugeType.EDGE_OUT;

            while (!(query instanceof ConditionQuery ||
                     query.originQuery() == null)) {
                /*
                 * Some backends(like RocksDB) may trans ConditionQuery to
                 * IdQuery or IdPrefixQuery, so we should get the origin query.
                 */
                query = query.originQuery();
            }

            if (!query.conditions().isEmpty() &&
                query instanceof ConditionQuery) {
                ConditionQuery cq = (ConditionQuery) query;
                // Does query IN edges
                if (cq.condition(HugeKeys.DIRECTION) == Directions.IN) {
                    type = HugeType.EDGE_IN;
                }
            }
        }

        return type;
    }

    public static final String joinTableName(String prefix, String table) {
        return prefix + "_" + table.toLowerCase();
    }

    public abstract void init(Session session);

    public abstract void clear(Session session);

    public abstract Iterator<BackendEntry> query(Session session, Query query);

    public abstract void insert(Session session, Entry entry);

    public abstract void delete(Session session, Entry entry);

    public abstract void append(Session session, Entry entry);

    public abstract void eliminate(Session session, Entry entry);

    /****************************** ShardSpliter ******************************/

    /**
     * 分片计算器
     * @param <Session>
     */
    public static abstract class ShardSpliter<Session extends BackendSession> {

        // The min shard size should >= 1M to prevent too many number of shards
        private static final int MIN_SHARD_SIZE = (int) Bytes.MB;   //分片大小

        // We assume the size of each key-value is 100 bytes
        private static final int ESTIMATE_BYTES_PER_KV = 100;

        private final String table;

        public ShardSpliter(String table) {
            this.table = table;
        }

        public String table() {
            return this.table;
        }

        /**
         * 计算分片
         * @param session
         * @param splitSize 分片大小
         * @return
         */
        public List<Shard> getSplits(Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);
            //估算总大小
            long size = this.estimateDataSize(session);
            if (size <= 0) {
                size = this.estimateNumKeys(session) * ESTIMATE_BYTES_PER_KV;
            }
            //估算分片数量
            double count = Math.ceil(size / (double) splitSize);
            if (count <= 0) {
                count = 1;
            }
            //分片token range分配
            //token 总数 UINT32_MAX
            //each : 每个shard的长度
            double each = BytesBuffer.UINT32_MAX / count;

            long offset = 0L;
            String last = this.position(offset);    //上一个offset
            List<Shard> splits = new ArrayList<>((int) count);
            while (offset < BytesBuffer.UINT32_MAX) {
                offset += each;
                if (offset > BytesBuffer.UINT32_MAX) {
                    offset = BytesBuffer.UINT32_MAX;
                }
                String current = this.position(offset);
                //创建分片
                splits.add(new Shard(last, current, 0L));
                last = current;
            }
            return splits;
        }

        public final String position(long position) {
            return String.valueOf(position);
        }
        //position must int
        public final byte[] position(String position) {
            int value = Long.valueOf(position).intValue();
            return NumericUtil.intToBytes(value);
        }

        /**
         * 估算空间大小
         * @param session
         * @return
         */
        protected abstract long estimateDataSize(Session session);

        /**
         * 估算Key数量
         * @param session
         * @return
         */
        protected abstract long estimateNumKeys(Session session);
    }
}
