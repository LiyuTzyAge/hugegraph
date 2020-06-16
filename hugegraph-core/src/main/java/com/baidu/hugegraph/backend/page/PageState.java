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

import java.util.Base64;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

/**
 * page的状态信息
 */
public class PageState {

    public static final byte[] EMPTY_BYTES = new byte[0];
    //代表第一个page
    public static final PageState EMPTY = new PageState(EMPTY_BYTES, 0, 0);

    private final byte[] position;
    private final int offset;   //column偏移量
    private final int total;

    /**
     *
     * @param position page rowkey字节，或Map结构的字符串，UTF-8编码
     * @param offset column偏移量
     * @param total 所有page返回的总量
     */
    public PageState(byte[] position, int offset, int total) {
        E.checkNotNull(position, "position");
        this.position = position;
        this.offset = offset;
        this.total = total;
    }

    public byte[] position() {
        return this.position;
    }

    public int offset() {
        return this.offset;
    }

    /**
     * page数据总量
     * @return
     */
    public long total() {
        return this.total;
    }

    /**
     * 返回page的字符串形式
     * @return
     */
    @Override
    public String toString() {
        if (Bytes.equals(this.position(), EMPTY_BYTES)) {
            return null;
        }
        return toString(this.toBytes());
    }

    /**
     * 格式[position.length+position+int+int]
     * position.length=2
     * @return
     */
    private byte[] toBytes() {
        assert this.position.length > 0;
        int length = 2 + this.position.length + 2 * BytesBuffer.INT_LEN;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeBytes(this.position);
        buffer.writeInt(this.offset);
        buffer.writeInt(this.total);
        return buffer.bytes();
    }

    /**
     * page 的字符串形式。base64加密
     * @param page
     * @return
     */
    public static PageState fromString(String page) {
        return fromBytes(toBytes(page));
    }

    /**
     * bytes 格式[position.length+position+int+int]
     * @param bytes page信息
     * @return
     */
    public static PageState fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return EMPTY;
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            //position+offset+total
            return new PageState(buffer.readBytes(), buffer.readInt(),
                                 buffer.readInt());
        } catch (Exception e) {
            throw new BackendException("Invalid page: '0x%s'",
                                       e, Bytes.toHex(bytes));
        }
    }

    public static String toString(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static byte[] toBytes(String page) {
        try {
            return Base64.getDecoder().decode(page);
        } catch (Exception e) {
            throw new BackendException("Invalid page: '%s'", e, page);
        }
    }
}
