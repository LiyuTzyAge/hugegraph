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

package com.baidu.hugegraph.backend.id;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.backend.id.Id.IdType;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;

public final class IdUtil {

    /**
     * Id写到存储端添加前缀
     * @param id
     * @return
     */
    public static String writeStoredString(Id id) {
        String idString;
        switch (id.type()) {
            case LONG:
            case STRING:
            case UUID:
                idString = IdGenerator.asStoredString(id);
                break;
            case EDGE:
                idString = EdgeId.asStoredString(id);
                break;
            default:
                throw new AssertionError("Invalid id type " + id.type());
        }
        return id.type().prefix() + idString;
    }

    /**
     * 读取存储端Id，去掉前缀
     * @param id
     * @return
     */
    public static Id readStoredString(String id) {
        IdType type = IdType.valueOfPrefix(id);
        String idContent = id.substring(1);
        switch (type) {
            case LONG:
            case STRING:
            case UUID:
                return IdGenerator.ofStoredString(idContent, type);
            case EDGE:
                return EdgeId.parseStoredString(idContent);
            default:
                throw new IllegalArgumentException("Invalid id: " + id);
        }
    }

    /**
     * 将Id转换成ByteBuffer，添加前缀
     * @param id
     * @return
     */
    public static Object writeBinString(Id id) {
        //+1,添加前缀
        int len = id.edge() ? BytesBuffer.BUF_EDGE_ID : id.length() + 1;
        BytesBuffer buffer = BytesBuffer.allocate(len).writeId(id);
        buffer.flip();
        return buffer.asByteBuffer();
    }

    /**
     * 将ByteBuffer转换成Id
     * @param id
     * @return
     */
    public static Id readBinString(Object id) {
        BytesBuffer buffer = BytesBuffer.wrap((ByteBuffer) id);
        return buffer.readId();
    }

    /**
     * Id转字符串，添加前缀
     * @param id
     * @return
     */
    public static String writeString(Id id) {
        return "" + id.type().prefix() + id.asObject();
    }

    /**
     * 根据Id字符串转换成对象
     * 去掉前缀
     * L12312312
     * Sabderc
     * @param id
     * @return
     */
    public static Id readString(String id) {
        //根据id前缀判断id类型
        IdType type = IdType.valueOfPrefix(id);
        String idContent = id.substring(1);
        switch (type) {
            case LONG:
                return IdGenerator.of(Long.parseLong(idContent));
            case STRING:
            case UUID:
                return IdGenerator.of(idContent, type == IdType.UUID);
            case EDGE:
                return EdgeId.parse(idContent);
            default:
                throw new IllegalArgumentException("Invalid id: " + id);
        }
    }

    public static String writeLong(Id id) {
        return String.valueOf(id.asLong());
    }

    public static Id readLong(String id) {
        return IdGenerator.of(Long.parseLong(id));
    }

    /**
     * 转义串联
     * 串联字符串结构为：String.format(%s%s%s,value,splitor,value)
     * 如果value中存在splitor将会被转化escape。
     * escape：aa>a>bbb=》aa`>a>bbb
     * @param splitor
     * @param escape
     * @param values
     * @return
     */
    public static String escape(char splitor, char escape, String... values) {
        StringBuilder escaped = new StringBuilder((values.length + 1) << 4);
        // Do escape for every item in values
        for (String value : values) {
            if (escaped.length() > 0) {
                escaped.append(splitor);
            }

            if (value.indexOf(splitor) == -1) {
                escaped.append(value);
                continue;
            }
            //将values中包含的特殊字符splitor转义成escape+splitor
            // Do escape for current item
            for (int i = 0, n = value.length(); i < n; i++) {
                char ch = value.charAt(i);
                if (ch == splitor) {
                    escaped.append(escape);
                }
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    /**
     * 反转义
     * escape：aa>a>bbb=》aa`>a>bbb
     * unescape:
     * 0:aa`>a => aa>a
     * 1:bbb = > bbb
     * @param id
     * @param splitor
     * @param escape
     * @return
     */
    public static String[] unescape(String id, String splitor, String escape) {
        /*
         * Note that the `splitor`/`escape` maybe special characters in regular
         * expressions, but this is a frequently called method, for faster
         * execution, we forbid the use of special characters as delimiter
         * or escape sign.
         * The `limit` param -1 in split method can ensure empty string be
         * splited to a part.
         */
        String[] parts = id.split("(?<!" + escape + ")" + splitor, -1);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = StringUtils.replace(parts[i], escape + splitor,
                                           splitor);
        }
        return parts;
    }
}
