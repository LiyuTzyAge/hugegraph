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

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractSerializer
                implements GraphSerializer, SchemaSerializer {

    /**
     * 底层数据转换
     * @param entry
     * @return
     */
    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    /**
     * 通过id查询底层数据Entry
     * @param type
     * @param id
     * @return
     */
    protected abstract BackendEntry newBackendEntry(HugeType type, Id id);

    /**
     * 三种查询
     * Query条件转换
     * @param type
     * @param id
     * @return
     */
    protected abstract Id writeQueryId(HugeType type, Id id);

    protected abstract Query writeQueryEdgeCondition(Query query);

    protected abstract Query writeQueryCondition(Query query);

    @Override
    public Query writeQuery(Query query) {
        HugeType type = query.resultType();

        //edge+conditions查询
        // Serialize edge condition query (TODO: add VEQ(for EOUT/EIN))
        if (type.isEdge() && !query.conditions().isEmpty()) {
            if (!query.ids().isEmpty()) {
                throw new BackendException("Not supported query edge by id " +
                                           "and by condition at the same time");
            }

            Query result = this.writeQueryEdgeCondition(query);
            if (result != null) {
                return result;
            }
        }

        //id查询
        // Serialize id in query
        if (query instanceof IdQuery && !query.ids().isEmpty()) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(this.writeQueryId(type, id));
            }
            query = result;
        }

        //Query查询
        // Serialize condition(key/value) in query
        if (query instanceof ConditionQuery && !query.conditions().isEmpty()) {
            query = this.writeQueryCondition(query);
        }

        return query;
    }
}
