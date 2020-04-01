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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Idfiable;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.StringEncoding;

/**
 * 序列化后bytes数据容器
 * BackendEntry对应一个Vertex
 * BackendColumn：
 * 1.对应一个Vertex的Property [name=property id,value = property bytes]
 * 2. 对应一个Edge的多个Propertyes [name=Edgeid,value=properties bytes]
 *
 * 面向Key-value系统
 * BackendColumn.name = key;
 * BackendColumn.value = value;
 */
public interface BackendEntry extends Idfiable {

    public static class BackendColumn implements Comparable<BackendColumn> {

        //name 保存EdgeId时无megic
        public byte[] name;
        public byte[] value;

        public static BackendColumn of(byte[] name, byte[] value) {
            BackendColumn col = new BackendColumn();
            col.name = name;
            col.value = value;
            return col;
        }

        @Override
        public String toString() {
            return String.format("%s=%s",
                                 StringEncoding.decode(name),
                                 StringEncoding.decode(value));
        }

        /**
         * 根据名称比较
         * @param other
         * @return
         */
        @Override
        public int compareTo(BackendColumn other) {
            if (other == null) {
                return 1;
            }
            return Bytes.compare(this.name, other.name);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BackendColumn)) {
                return false;
            }
            BackendColumn other = (BackendColumn) obj;
            return Bytes.equals(this.name, other.name) &&
                   Bytes.equals(this.value, other.value);
        }
    }

    public HugeType type();

    @Override
    public Id id();

    public Id subId();

    public int columnsSize();
    public Collection<BackendColumn> columns();

    public void columns(Collection<BackendColumn> columns);
    public void columns(BackendColumn... columns);

    public void merge(BackendEntry other);

    public void clear();

    /**
     * 确认column是否属于当前Entry
     * column的name由（id与名称）拼接而成，如property的id
     * @param column
     * @return
     */
    public default boolean belongToMe(BackendColumn column) {
        return Bytes.prefixWith(column.name, id().asBytes());
    }

    public interface BackendIterator<T> extends Iterator<T> {

        public void close();

        public byte[] position();
    }

    public interface BackendColumnIterator
           extends BackendIterator<BackendColumn> {

        public static BackendColumnIterator empty() {
            return EMPTY;
        }

        public final BackendColumnIterator EMPTY = new BackendColumnIterator() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public BackendColumn next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                // pass
            }

            @Override
            public byte[] position() {
                return null;
            }
        };
    }

    public static class BackendColumnIteratorWrapper
           implements BackendColumnIterator {

        private final Iterator<BackendColumn> iter;

        public BackendColumnIteratorWrapper(BackendColumn... cols) {
            this.iter = Arrays.asList(cols).iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public BackendColumn next() {
            return iter.next();
        }

        @Override
        public void close() {
            // pass
        }

        @Override
        public byte[] position() {
            return null;
        }
    }
}
