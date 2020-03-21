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

import java.util.function.Consumer;
import java.util.function.Function;

import com.baidu.hugegraph.backend.id.Id;

/**
 * 缓存的标准接口
 */
public interface Cache {

    /**
     * get object by id
     * @param id
     * @return
     */
    public Object get(Id id);

    /**
     * the object get or fetch from fetcher by id
     * @param id
     * @param fetcher
     * @return
     */
    public Object getOrFetch(Id id, Function<Id, Object> fetcher);

    /**
     * update cache by id and object
     * @param id
     * @param value
     */
    public void update(Id id, Object value);

    public void updateIfAbsent(Id id, Object value);

    public void updateIfPresent(Id id, Object value);

    public void invalidate(Id id);

    /**
     * traverse all object of cache and accept the consumer
     * @param consumer function
     */
    public void traverse(Consumer<Object> consumer);

    public void clear();

    /**
     * the expire of the cache
     * @param seconds
     */
    public void expire(long seconds);

    /**
     * get expire
     * @return
     */
    public long expire();

    /**
     * ?
     * @return
     */
    public long tick();

    /**
     * get capacity of iterms
     * @return
     */
    public long capacity();

    /**
     * get the size of object
     * @return
     */
    public long size();

    public long hits();

    public long miss();
}
