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

package com.baidu.hugegraph.backend.query;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

/**
 * 由某个ID开始query，并且匹配前缀
 * 用于EdgeId查询
 */
public final class IdPrefixQuery extends Query {
    //ownerVertex+dir+labelId+sortValue
    private final Id start; //开始点，BinaryId
    private final boolean inclusiveStart;   //是否包含startId
    //ownerVertex+dir+labelId
    private final Id prefix; //前缀，BinaryId

    public IdPrefixQuery(HugeType resultType, Id prefix) {
        this(resultType, null, prefix, true, prefix);
    }

    /**
     * 根据一个前缀匹配
     * @param originQuery
     * @param prefix
     */
    public IdPrefixQuery(Query originQuery, Id prefix) {
        this(originQuery.resultType(), originQuery, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id start, Id prefix) {
        this(originQuery.resultType(), originQuery, start, true, prefix);
    }

    public IdPrefixQuery(Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        this(originQuery.resultType(), originQuery, start, inclusive, prefix);
    }

    /**
     *
     * @param resultType 查询结果类型
     * @param originQuery 父级查询
     * @param start 查询开始Id
     * @param inclusive 是否包含start
     * @param prefix 查询条件，前缀
     */
    public IdPrefixQuery(HugeType resultType, Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.inclusiveStart = inclusive;
        this.prefix = prefix;
        if (originQuery != null) {
            this.copyBasic(originQuery);
        }
    }

    public Id start() {
        return this.start;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public Id prefix() {
        return this.prefix;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(HugeElement element) {
        byte[] elem = element.id().asBytes();
        int cmp = Bytes.compare(elem, this.start.asBytes());
        //需要大于起始点
        boolean matchedStart = this.inclusiveStart ? cmp >= 0 : cmp > 0;
        //比较前缀每个字节，满足前缀
        boolean matchedPrefix = Bytes.prefixWith(elem, this.prefix.asBytes());
        return matchedStart && matchedPrefix;
    }

    @Override
    public IdPrefixQuery copy() {
        return (IdPrefixQuery) super.copy();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        assert sb.length() > 0;
        sb.deleteCharAt(sb.length() - 1); // Remove the last "`"
        sb.append(" id prefix with ").append(this.prefix);
        if (this.start != this.prefix) {
            sb.append(" and start with ").append(this.start)
              .append("(")
              .append(this.inclusiveStart ? "inclusive" : "exclusive")
              .append(")");
        }
        sb.append("`");
        return sb.toString();
    }
}
