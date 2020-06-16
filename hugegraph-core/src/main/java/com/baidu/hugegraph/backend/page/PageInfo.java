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
import java.util.Iterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

/**
 * PageState描述page的索引信息
 * PageInfo 在pageState基础上增加offset
 * offset代表正在执行的subQuery，因一个复杂Query会被分解为多个or关系的subQuery。
 * 执行流程：先执行第一个subQuery，返回所有数据后，执行下一个。page信息表示当前subQuery读取的数据位置position
 * 。切换下一个subQuery将会从开始(PAGE_NONE)，重新读取数据
 */
public final class PageInfo {

    public static final String PAGE = "page";
    //page信息为空，代表重头开始读取数据
    public static final String PAGE_NONE = "";

    private int offset;     //当前subQuery偏移量
    private String page;    //page信息base64

    public PageInfo(int offset, String page) {
        E.checkArgument(offset >= 0, "The offset must be >= 0");
        E.checkNotNull(page, "page");
        this.offset = offset;
        this.page = page;
    }

    /**
     * 移动subQuery指针，重置gage，由开始重新读取
     */
    public void increase() {
        this.offset++;
        this.page = PAGE_NONE;
    }

    public int offset() {
        return this.offset;
    }

    public void page(String page) {
        this.page = page;
    }

    public String page() {
        return this.page;
    }

    @Override
    public String toString() {
        return Base64.getEncoder().encodeToString(this.toBytes());
    }

    /**
     * pageInfo=
     * [offset+pageState.length+pageState.bytes]
     * [int(4)+short(2)+bytes]
     * @return
     */
    public byte[] toBytes() {
        byte[] pageState = PageState.toBytes(this.page);
        int length = 2 + BytesBuffer.INT_LEN + pageState.length;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeInt(this.offset);
        buffer.writeBytes(pageState);
        return buffer.bytes();
    }

    public static PageInfo fromString(String page) {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(page);
        } catch (Exception e) {
            throw new HugeException("Invalid page: '%s'", e, page);
        }
        return fromBytes(bytes);
    }

    /**
     * pageInfo=
     * [offset+pageState.length+pageState.bytes]
     * [int(4)+short(2)+bytes]
     * @param bytes
     * @return
     */
    public static PageInfo fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return new PageInfo(0, PAGE_NONE);
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            int offset = buffer.readInt();
            byte[] pageState = buffer.readBytes();
            String page = PageState.toString(pageState);
            return new PageInfo(offset, page);
        } catch (Exception e) {
            throw new HugeException("Invalid page: '0x%s'",
                                    e, Bytes.toHex(bytes));
        }
    }

    /**
     * 将PageEntryIterator 转换为 PageState，索引信息
     * 同时会更新PageState中position信息
     * @param iterator
     * @return
     */
    public static PageState pageState(Iterator<?> iterator) {
        E.checkState(iterator instanceof Metadatable,
                     "Invalid paging iterator: %s", iterator.getClass());
        //为iterator 生成pageState信息，更新position
        Object page = ((Metadatable) iterator).metadata(PAGE);
        E.checkState(page instanceof PageState,
                     "Invalid PageState '%s'", page);
        return (PageState) page;
    }

    /**
     * 将PageEntryIterator 转换为 page字符串信息，即索引信息
     * @param iterator
     * @return
     */
    public static String pageInfo(Iterator<?> iterator) {
        E.checkState(iterator instanceof Metadatable,
                     "Invalid paging iterator: %s", iterator.getClass());
        Object page = ((Metadatable) iterator).metadata(PAGE);
        return page == null ? null : page.toString();
    }
}
