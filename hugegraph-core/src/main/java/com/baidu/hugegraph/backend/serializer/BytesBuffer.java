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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.Id.IdType;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.KryoUtil;
import com.baidu.hugegraph.util.StringEncoding;

/**
 * class BytesBuffer is a util for read/write binary
 * 对象序列化工具（Vertex、Edge、ID）等
 */
public final class BytesBuffer {

    public static final int BYTE_LEN = Byte.BYTES;  //字节长度 1
    public static final int SHORT_LEN = Short.BYTES;    //2byte
    public static final int INT_LEN = Integer.BYTES;    //4byte
    public static final int LONG_LEN = Long.BYTES;
    public static final int CHAR_LEN = Character.BYTES;
    public static final int FLOAT_LEN = Float.BYTES;
    public static final int DOUBLE_LEN = Double.BYTES;

    public static final int UINT8_MAX = ((byte) -1) & 0xff; //1111 1111
    public static final int UINT16_MAX = ((short) -1) & 0xffff; //1111 1111 1111 1111
    public static final long UINT32_MAX = (-1) & 0xffffffffL;

    public static final int ID_LEN_MASK = 0x7f;
    // NOTE: +1 to let code 0 represent length 1
    public static final int ID_LEN_MAX = 0x7f + 1; // 128
    //Id最大长度
    public static final int BIG_ID_LEN_MAX = 0x7fff + 1; // 32768
    // 字节最大值，代表字符串结尾或null
    public static final byte STRING_ENDING_BYTE = (byte) 0xff;  //byte

    public static final int STRING_LEN_MAX = UINT16_MAX;

    //字符串Hash类型index id字节最大长度
    // The value must be in range [8, ID_LEN_MAX]
    public static final int INDEX_HASH_ID_THRESHOLD = 32;

    public static final int DEFAULT_CAPACITY = 64;      //默认容量
    public static final int MAX_BUFFER_CAPACITY = 128 * 1024 * 1024; // 128M

    public static final int BUF_EDGE_ID = 128;      //edge id 长度
    public static final int BUF_PROPERTY = 64;      //property value 初始化长度

    /**
     * ByteBuffer nio 缓存
     */
    private ByteBuffer buffer;

    public BytesBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public BytesBuffer(int capacity) {
        E.checkArgument(capacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public BytesBuffer(ByteBuffer buffer) {
        E.checkNotNull(buffer, "buffer");
        this.buffer = buffer;
    }

    public static BytesBuffer allocate(int capacity) {
        return new BytesBuffer(capacity);
    }

    public static BytesBuffer wrap(ByteBuffer buffer) {
        return new BytesBuffer(buffer);
    }

    public static BytesBuffer wrap(byte[] array) {
        return new BytesBuffer(ByteBuffer.wrap(array));
    }

    public static BytesBuffer wrap(byte[] array, int offset, int length) {
        return new BytesBuffer(ByteBuffer.wrap(array, offset, length));
    }

    public ByteBuffer asByteBuffer() {
        return this.buffer;
    }

    public BytesBuffer flip() {
        this.buffer.flip();
        return this;
    }

    public byte[] array() {
        return this.buffer.array();
    }

    /**
     * 返回position内有效的字节数组
     * @return
     */
    public byte[] bytes() {
        byte[] bytes = this.buffer.array();
        if (this.buffer.position() == bytes.length) {
            return bytes;
        } else {
            return Arrays.copyOf(bytes, this.buffer.position());
        }
    }

    public BytesBuffer copyFrom(BytesBuffer other) {
        return this.write(other.bytes());
    }

    public int remaining() {
        return this.buffer.remaining();
    }

    /**
     * 向缓存申请空间
     * @param size
     */
    private void require(int size) {
        // Does need to resize?
        if (this.buffer.limit() - this.buffer.position() >= size) {
            return;
        }

        // Extra capacity as buffer
        int newcapacity = size + this.buffer.limit() + DEFAULT_CAPACITY;
        E.checkArgument(newcapacity <= MAX_BUFFER_CAPACITY,
                        "Capacity exceeds max buffer capacity: %s",
                        MAX_BUFFER_CAPACITY);
        //重建缓存
        ByteBuffer newBuffer = ByteBuffer.allocate(newcapacity);
        //翻转缓存，准备写入
        this.buffer.flip();
        //此方法将给定源缓冲区中剩余的字节传输到此缓冲区
        newBuffer.put(this.buffer);
        this.buffer = newBuffer;
    }

    public BytesBuffer write(byte val) {
        require(BYTE_LEN);
        this.buffer.put(val);
        return this;
    }

    /**
     * 写入一个字节
     * @param val
     * @return
     */
    public BytesBuffer write(int val) {
        assert val <= UINT8_MAX;
        require(BYTE_LEN);
        this.buffer.put((byte) val);
        return this;
    }

    /**
     * 向缓存写入数据
     * @param val
     * @return
     */
    public BytesBuffer write(byte[] val) {
        //申请空间
        require(BYTE_LEN * val.length);
        //写入字节数据
        this.buffer.put(val);
        return this;
    }

    public BytesBuffer write(byte[] val, int offset, int length) {
        require(BYTE_LEN * length);
        this.buffer.put(val, offset, length);
        return this;
    }

    public BytesBuffer writeBoolean(boolean val) {
        return this.write(val ? 1 : 0);
    }

    public BytesBuffer writeChar(char val) {
        require(CHAR_LEN);
        this.buffer.putChar(val);
        return this;
    }

    public BytesBuffer writeShort(short val) {
        require(SHORT_LEN);
        this.buffer.putShort(val);
        return this;
    }

    public BytesBuffer writeInt(int val) {
        require(INT_LEN);
        this.buffer.putInt(val);
        return this;
    }

    public BytesBuffer writeLong(long val) {
        require(LONG_LEN);
        this.buffer.putLong(val);
        return this;
    }

    public BytesBuffer writeFloat(float val) {
        require(FLOAT_LEN);
        this.buffer.putFloat(val);
        return this;
    }

    public BytesBuffer writeDouble(double val) {
        require(DOUBLE_LEN);
        this.buffer.putDouble(val);
        return this;
    }

    public byte peek() {
        return this.buffer.get(this.buffer.position());
    }

    public byte peekLast() {
        return this.buffer.get(this.buffer.capacity() - 1);
    }

    /**
     * 读取一个Byte
     * @return
     */
    public byte read() {
        return this.buffer.get();
    }

    /**
     * 读取多个byte
     * @param length 字节长度
     * @return
     */
    public byte[] read(int length) {
        byte[] bytes = new byte[length];
        this.buffer.get(bytes);
        return bytes;
    }

    public boolean readBoolean() {
        return this.buffer.get() == 0 ? false : true;
    }

    public char readChar() {
        return this.buffer.getChar();
    }

    public short readShort() {
        return this.buffer.getShort();
    }

    /**
     * 4byte
     * @return
     */
    public int readInt() {
        return this.buffer.getInt();
    }

    public long readLong() {
        return this.buffer.getLong();
    }

    public float readFloat() {
        return this.buffer.getFloat();
    }

    public double readDouble() {
        return this.buffer.getDouble();
    }

    public BytesBuffer writeBytes(byte[] bytes) {
        E.checkArgument(bytes.length <= UINT16_MAX,
                        "The max length of bytes is %s, but got %s",
                        UINT16_MAX, bytes.length);
        require(SHORT_LEN + bytes.length);
        this.writeVInt(bytes.length);
        this.write(bytes);
        return this;
    }

    /**
     * bytes 底层结构【length+bytes】
     * @return
     */
    public byte[] readBytes() {
        int length = this.readVInt();
        assert length >= 0;
        byte[] bytes = this.read(length);
        return bytes;
    }

    /**
     * Str 编码 UTF-8
     * @param val
     * @return
     */
    public BytesBuffer writeStringRaw(String val) {
        this.write(StringEncoding.encode(val));
        return this;
    }

    public BytesBuffer writeString(String val) {
        byte[] bytes = StringEncoding.encode(val);
        this.writeBytes(bytes);
        return this;
    }

    public String readString() {
        return StringEncoding.decode(this.readBytes());
    }

    /**
     * 写入字符串数据，并添加字符串结尾0xff
     * 如果val为空，只写入字符串结尾
     * @param val 字符串结尾
     * @return
     */
    public BytesBuffer writeStringWithEnding(String val) {
        if (!val.isEmpty()) {
            byte[] bytes = StringEncoding.encode(val);
            // assert '0xff' not exist in string-id-with-ending (utf8 bytes)
            assert !Bytes.contains(bytes, STRING_ENDING_BYTE);
            this.write(bytes);
        }
        /*
         * A reasonable ending symbol should be 0x00(to ensure order), but
         * considering that some backends like PG do not support 0x00 string,
         * so choose 0xFF currently.
         */
        this.write(STRING_ENDING_BYTE);
        return this;
    }

    public String readStringWithEnding() {
        return StringEncoding.decode(this.readBytesWithEnding());
    }

    public BytesBuffer writeStringToRemaining(String value) {
        byte[] bytes = StringEncoding.encode(value);
        this.write(bytes);
        return this;
    }

    public String readStringFromRemaining() {
        byte[] bytes = new byte[this.buffer.remaining()];
        this.buffer.get(bytes);
        return StringEncoding.decode(bytes);
    }

    /**
     * 写入一个字节
     * @param val
     * @return
     */
    public BytesBuffer writeUInt8(int val) {
        assert val <= UINT8_MAX;        //小于8位最大值
        this.write(val);
        return this;
    }

    public int readUInt8() {
        return this.read() & 0x000000ff;    //11111111
    }

    /**
     * 序列化16bit的数值
     * @param val
     * @return
     */
    public BytesBuffer writeUInt16(int val) {
        assert val <= UINT16_MAX;
        this.writeShort((short) val);
        return this;
    }

    public int readUInt16() {
        return this.readShort() & 0x0000ffff;
    }

    public BytesBuffer writeUInt32(long val) {
        assert val <= UINT32_MAX;
        this.writeInt((int) val);
        return this;
    }

    public long readUInt32() {
        return this.readInt() & 0xffffffffL;
    }

    /**
     * int类型序列化
     * 根据value大小决定写入几个byte，最少写入1个，最大写入5个
     * 可以实现边写入边压缩
     * 如果数值较大，截断分多次写入，第一次写入4bit，之后每次写入7bit
     * (0x80|) 高位=0 代表数据结尾，=1代表后面还有数据
     * @param value 有符号整数
     * @return
     */
    public BytesBuffer writeVInt(int value) {
        // 0x7f=01111111
        // NOTE: negative numbers are not compressed
        if (value > 0x0fffffff || value < 0) {
            //写入实际数据高4bit[1bit-4bit]=4bit
            //1000 xxxx
            this.write(0x80 | ((value >>> 28) & 0x7f));
        }
        if (value > 0x1fffff || value < 0) {
            //写入[高5bit-高11bit]=7bit
            //1xxx xxxx
            this.write(0x80 | ((value >>> 21) & 0x7f));
        }
        if (value > 0x3fff || value < 0) {
            //写入[高12bit-高18bit]=7bit
            //1xxx xxxx
            this.write(0x80 | ((value >>> 14) & 0x7f));
        }
        if (value > 0x7f || value < 0) {
            //写入[高19bit-高25bit]=7bit
            //1xxx xxxx
            this.write(0x80 | ((value >>>  7) & 0x7f));
        }
        //写入[高26bit-高32bit]=7bit
        //0xxx xxxx
        this.write(value & 0x7f);

        return this;
    }

    /**
     * int 反序列化
     * @return
     */
    public int readVInt() {
        byte leading = this.read();
        //正常数据中不存在 0x80
        E.checkArgument(leading != 0x80,
                        "Unexpected varint with leading byte '0x%s'",
                        Bytes.toHex(leading));
        int value = leading & 0x7f;     //& 0x7f=01111111=去掉了第一个字符
        if (leading >= 0) {      //0xxx xxxx
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 5; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);       //添加低7bit
                break;
            } else {
                value = (b & 0x7f) | (value << 7);  //去掉了第一个字符,添加低7bit
            }
        }

        E.checkArgument(i < 5,
                        "Unexpected varint %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 4 || (leading & 0x70) == 0,
                        "Unexpected varint %s with leading byte '0x%s'",
                        value, Bytes.toHex(leading));
        return value;
    }

    /**
     * 序列化方式与int类似，最高位标识结尾，每次写入7bit
     * @param value
     * @return
     */
    public BytesBuffer writeVLong(long value) {
        if (value < 0) {
            this.write((byte) 0x81);
        }
        if (value > 0xffffffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 56) & 0x7f));
        }
        if (value > 0x1ffffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 49) & 0x7f));
        }
        if (value > 0x3ffffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 42) & 0x7f));
        }
        if (value > 0x7ffffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 35) & 0x7f));
        }
        if (value > 0xfffffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 28) & 0x7f));
        }
        if (value > 0x1fffffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 21) & 0x7f));
        }
        if (value > 0x3fffL || value < 0L) {
            this.write(0x80 | ((int) (value >>> 14) & 0x7f));
        }
        if (value > 0x7fL || value < 0L) {
            this.write(0x80 | ((int) (value >>>  7) & 0x7f));
        }
        this.write((int) value & 0x7f);

        return this;
    }

    public long readVLong() {
        byte leading = this.read();
        E.checkArgument(leading != 0x80,
                        "Unexpected varlong with leading byte '0x%s'",
                        Bytes.toHex(leading));
        long value = leading & 0x7fL;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 10; i++) {
            byte b = this.read();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }

        E.checkArgument(i < 10,
                        "Unexpected varlong %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 9 || (leading & 0x7e) == 0,
                        "Unexpected varlong %s with leading byte '0x%s'",
                        value, Bytes.toHex(leading));
        return value;
    }

    /**
     * 序列化 property 值
     * @param pkey
     * @param value
     * @return
     */
    public BytesBuffer writeProperty(PropertyKey pkey, Object value) {
        if (pkey.cardinality() == Cardinality.SINGLE) {
            this.writeProperty(pkey.dataType(), value);
            return this;
        }

        assert pkey.cardinality() == Cardinality.LIST ||
               pkey.cardinality() == Cardinality.SET;
        Collection<?> values = (Collection<?>) value;
        this.writeVInt(values.size());
        for (Object o : values) {
            this.writeProperty(pkey.dataType(), o);
        }
        return this;
    }

    /**
     * 反序列化 Property 值
     * SINGLE 模式：直接返回property值
     * Collection 模式：返回Collection
     * @param pkey
     * @return
     */
    public Object readProperty(PropertyKey pkey) {
        if (pkey.cardinality() == Cardinality.SINGLE) {
            return this.readProperty(pkey.dataType());  //pkey.dataType()
            // 返回value内部数据类型
        }

        assert pkey.cardinality() == Cardinality.LIST ||
               pkey.cardinality() == Cardinality.SET;
        //符合序列化数据结构： size+data[]
        int size = this.readVInt();
        Collection<Object> values = pkey.newValue();    //返回value的实例
        for (int i = 0; i < size; i++) {
            values.add(this.readProperty(pkey.dataType()));
        }
        return values;
    }

    /**
     * 根据id类型写入id，ElementId或SchemaId
     * @param id
     * @return
     */
    public BytesBuffer writeId(Id id) {
        return this.writeId(id, false);
    }

    /**
     *
     * 根据id类型写入id（Long,String,UUID,EDGE）
     * Id 包括(VertexId，EdgeId），也包括系统属性id（LabelId等）
     * 注：EdgeId 包含Megic
     * @param id
     * @param big id是否为长id（短：<=128，长：<=32768）
     * @return
     */
    public BytesBuffer writeId(Id id, boolean big) {
        switch (id.type()) {
            case LONG:
                // Number Id
                long value = id.asLong();
                this.writeNumber(value);
                break;
            case UUID:
                // UUID Id
                byte[] bytes = id.asBytes();
                assert bytes.length == Id.UUID_LENGTH;
                this.writeUInt8(0x7f); // 0b01111111 means UUID 类型头
                this.write(bytes);  //写入数据
                break;
            case EDGE:
                // Edge Id
                this.writeUInt8(0x7e); // 0b01111110 means EdgeId 类型头
                this.writeEdgeId(id);
                break;
            default:
                //字符串写入流程 : 写入长度,数据
                // 数据结构：id长度+实际数据[id.bytes]
                // short Id:[writeUInt8(len | 0x80)]+实际数据[id.bytes]
                // long Id::[len(high)_len(low)]+实际数据[id.bytes]
                // String Id
                bytes = id.asBytes();
                int len = bytes.length;
                E.checkArgument(len > 0, "Can't write empty id");
                //id是否为长id
                //high | 0x80 功能：在头上补上1码，数值相当于加上0x80
                if (!big) {
                    E.checkArgument(len <= ID_LEN_MAX,   //(0x7f + 1)=0x80=128
                                    "Id max length is %s, but got %s {%s}",
                                    ID_LEN_MAX, len, id);
                    // 源 [1, 128] to 目标 [0, 127] 不与补码冲突
                    len -= 1;
                    /*
                    0x80=128 , len [0,127]
                    len | 0x80 、len < 0x80 --> 0x80<=结果<=255
                    写入id长度
                    len | 0x80 --> len+128
                     */
                    this.writeUInt8(len | 0x80);
                } else {
                    //BIG_ID_LEN_MAX 0x7fff + 1;=0x8000=32768 ; 16bit
                    E.checkArgument(len <= BIG_ID_LEN_MAX,
                                    "Big id max length is %s, but got %s {%s}",
                                    BIG_ID_LEN_MAX, len, id);
                    len -= 1; //[0,BIG_ID_LEN_MAX] 16bit
                    int high = len >> 8;       //high 8bit
                    int low = len & 0xff;       // low 8bit
                    this.writeUInt8(high | 0x80);  //high | 0x80 加上0x80
                    this.writeUInt8(low);
                }
                this.write(bytes);  //写入数据
                break;
        }
        return this;
    }

    public Id readId() {
        return this.readId(false);
    }

    /**
     * Id 反序列化 （UUID、EdgeId、NumberId/StringId）
     * 注：EdgeId 包含 megic
     * @param big
     * @return
     */
    public Id readId(boolean big) {
        byte b = this.read();
        //判断megic first bit
        boolean number = (b & 0x80) == 0;
        if (number) {
            if (b == 0x7f) {
                // UUID Id
                return IdGenerator.of(this.read(Id.UUID_LENGTH), IdType.UUID);
            } else if (b == 0x7e) {
                // Edge Id
                return this.readEdgeId();
            } else {
                // Number Id
                return IdGenerator.of(this.readNumber(b));
            }
        } else {
            // String Id
            int len = b & ID_LEN_MASK;
            if (big) {
                int high = len << 8;
                int low = this.readUInt8();
                len = high + low;
            }
            len += 1; // restore [0, 127] to [1, 128]
            byte[] id = this.read(len);
            return IdGenerator.of(id, IdType.STRING);
        }
    }

    /**
     * Edge id 序列化，不包含Megic
     * 格式 owner-vertex + directory + edge-label + [sort-values+0xff] +
     * other-vertex
     * @param id
     * @return
     */
    public BytesBuffer writeEdgeId(Id id) {
        EdgeId edge = (EdgeId) id;
        this.writeId(edge.ownerVertexId());
        this.write(edge.directionCode());
        this.writeId(edge.edgeLabelId());
        //使用0xff代表是否为空
        this.writeStringWithEnding(edge.sortValues());
        this.writeId(edge.otherVertexId());
        return this;
    }

    /**
     * 反序列化Edge id，不包含Megic
     * @return
     */
    public Id readEdgeId() {
        return new EdgeId(this.readId(), EdgeId.directionFromCode(this.read()),
                          this.readId(), this.readStringWithEnding(),
                          this.readId());
    }

    /**
     * 索引id转BytesBuffer
     * @param id
     * @param type
     * @return
     */
    public BytesBuffer writeIndexId(Id id, HugeType type) {
        return this.writeIndexId(id, type, true);
    }

    /**
     * 索引id写入BytesBuffer,根据withEndIng写入结尾
     * @param id
     * @param type
     * @param withEnding 是否写入字符串结尾
     * @return
     */
    public BytesBuffer writeIndexId(Id id, HugeType type, boolean withEnding) {
        byte[] bytes = id.asBytes();
        int len = bytes.length;
        E.checkArgument(len > 0, "Can't write empty id");

        this.write(bytes);
        if (type.isStringIndex()) {
            // Not allow '0xff' exist in string-id-with-ending
            E.checkArgument(!Bytes.contains(bytes, STRING_ENDING_BYTE),
                            "The %s type index id can't contains " +
                            "byte '0x%s', but got: 0x%s", type,
                            Bytes.toHex(STRING_ENDING_BYTE),
                            Bytes.toHex(bytes));
            if (withEnding) {
                this.writeStringWithEnding("");
            }
        }
        return this;
    }

    /**
     * 反序列化 IndexId
     * @param type
     * @return BinaryId
     */
    public BinaryId readIndexId(HugeType type) {
        byte[] id;
        if (type.isRange4Index()) {
            // IndexLabel 4 bytes + fieldValue 4 bytes
            id = this.read(8);
        } else if (type.isRange8Index()) {
            // IndexLabel 4 bytes + fieldValue 8 bytes
            id = this.read(12);
        } else {
            assert type.isStringIndex();
            id = this.readBytesWithEnding();
        }
        return new BinaryId(id, IdGenerator.of(id, IdType.STRING));
    }

    public BinaryId asId() {
        return new BinaryId(this.bytes(), null);
    }

    public BinaryId parseId() {
        // Parse id from bytes
        int start = this.buffer.position();
        Id id = this.readId();
        int end = this.buffer.position();
        int len = end - start;
        byte[] bytes = new byte[len];
        System.arraycopy(this.array(), start, bytes, 0, len);
        return new BinaryId(bytes, id);
    }

    private void writeNumber(long val) {
        /*
         * 8 kinds of number, 2 ~ 9 bytes number:
         * 0b 0kkksxxx X...
         * 0(1 bit) + kind(3 bits) + signed(1 bit) + number(n bits)
         *
         * 2 byte : 0b 0000 1xxx X(8 bits)                  [0, 2047]
         *          0b 0000 0xxx X(8 bits)                  [-2048, -1]
         * 3 bytes: 0b 0001 1xxx X X                        [0, 524287]
         *          0b 0001 0xxx X X                        [-524288, -1]
         * 4 bytes: 0b 0010 1xxx X X X                      [0, 134217727]
         *          0b 0010 0xxx X X X                      [-134217728, -1]
         * 5 bytes: 0b 0011 1xxx X X X X                    [0, 2^35 - 1]
         *          0b 0011 0xxx X X X X                    [-2^35, -1]
         * 6 bytes: 0b 0100 1xxx X X X X X                  [0, 2^43 - 1]
         *          0b 0100 0xxx X X X X X                  [-2^43, -1]
         * 7 bytes: 0b 0101 1xxx X X X X X X                [0, 2^51 - 1]
         *          0b 0101 0xxx X X X X X X                [-2^51, -1]
         * 8 bytes: 0b 0110 1xxx X X X X X X X              [0, 2^59 - 1]
         *          0b 0110 0xxx X X X X X X X              [-2^59, -1]
         * 9 bytes: 0b 0111 1000 X X X X X X X X            [0, 2^64 - 1]
         *          0b 0111 0000 X X X X X X X X            [-2^64, -1]
         *
         * NOTE:    0b 0111 1111 is used by 128 bits UUID
         *          0b 0111 1110 is used by EdgeId
         */
        int positive = val >= 0 ? 0x08 : 0x00;
        if (~0x7ffL <= val && val <= 0x7ffL) { //~0x7ffL = 2047; 0x7ffL=2047
            //2 byte
            //截取11bit之内的数据->4+1+11=16bit
            int high3bits = (int) (val >> 8) & 0x07;    //取高位3bit值 singlexxx
            //0x00 | positive | high3bits
            //拼接Head (0(1 bit) + kind(3 bits) + signed(1 bit)) + xxx
            this.writeUInt8(0x00 | positive | high3bits); //写入前8bit
            this.writeUInt8((byte) val);    //写入后8bit
        } else if (~0x7ffffL <= val && val <= 0x7ffffL) {
            //3 byte
            //截取17bit之内的数据->4+1+11=16bit
            int high3bits = (int) (val >> 16) & 0x07;
            this.writeUInt8(0x10 | positive | high3bits);
            this.writeShort((short) val);
        } else if (~0x7ffffffL <= val && val <= 0x7ffffffL) {
            //4 byte
            int high3bits = (int) (val >> 24 & 0x07);
            this.writeUInt8(0x20 | positive | high3bits);
            this.write((byte) (val >> 16));
            this.writeShort((short) val);
        } else if (~0x7ffffffffL <= val && val <= 0x7ffffffffL) {
            //5 byte
            int high3bits = (int) (val >> 32) & 0x07;
            this.writeUInt8(0x30 | positive | high3bits);
            this.writeInt((int) val);
        } else if (~0x7ffffffffffL <= val && val <= 0x7ffffffffffL) {
            //6 byte
            int high3bits = (int) (val >> 40) & 0x07;
            this.writeUInt8(0x40 | positive | high3bits);
            this.write((byte) (val >> 32));
            this.writeInt((int) val);
        } else if (~0x7ffffffffffffL <= val && val <= 0x7ffffffffffffL) {
            //7 byte
            int high3bits = (int) (val >> 48) & 0x07;
            this.writeUInt8(0x50 | positive | high3bits);
            this.writeShort((short) (val >> 32));
            this.writeInt((int) val);
        } else if (~0x7ffffffffffffffL <= val && val <= 0x7ffffffffffffffL) {
            //8 byte
            int high3bits = (int) (val >> 56) & 0x07;
            this.writeUInt8(0x60 | positive | high3bits);
            this.write((byte) (val >> 48));
            this.writeShort((short) (val >> 32));
            this.writeInt((int) val);
        } else {
            //9 byte
            // high3bits is always 0b000 for 9 bytes number
            this.writeUInt8(0x70 | positive);   //0111 1000/0111 0000
            this.writeLong(val);
        }
    }

    /**
     * just for test
     * @param val
     */
    public void writeNumberTest(long val)
    {
        writeNumber(val);
    }

    /**
     * 反序列化Long number
     * @param b
     * @return
     */
    private long readNumber(byte b) {
        E.checkArgument((b & 0x80) == 0,
                        "Not a number type with prefix byte '0x%s'",
                        Bytes.toHex(b));
        // Parse the kind from byte 0kkksxxx
        int kind = b >>> 4;
        boolean positive = (b & 0x08) > 0;
        long high3bits = b & 0x07;
        long value = high3bits << ((kind + 1) * 8);
        switch (kind) {
            case 0:
                value |= this.readUInt8();
                break;
            case 1:
                value |= this.readUInt16();
                break;
            case 2:
                value |= this.readUInt8() << 16 | this.readUInt16();
                break;
            case 3:
                value |= this.readUInt32();
                break;
            case 4:
                value |= (long) this.readUInt8() << 32 | this.readUInt32();
                break;
            case 5:
                value |= (long) this.readUInt16() << 32 | this.readUInt32();
                break;
            case 6:
                value |= (long) this.readUInt8() << 48 |
                         (long) this.readUInt16() << 32 |
                         this.readUInt32();
                break;
            case 7:
                assert high3bits == 0L;
                value |= this.readLong();
                break;
            default:
                throw new AssertionError("Invalid length of number: " + kind);
        }
        if (!positive && kind < 7) {
            // Restore the bits of the original negative number
            long mask = Long.MIN_VALUE >> (52 - kind * 8);
            value |= mask;
        }
        return value;
    }

    /**
     * 读取bytes直到 STRING_ENDING_BYTE
     * @return
     */
    private byte[] readBytesWithEnding() {
        int start = this.buffer.position();
        boolean foundEnding =false;
        byte current;
        while (this.remaining() > 0) {
            current = this.read();
            if (current == STRING_ENDING_BYTE) {
                foundEnding = true;
                break;
            }
        }
        E.checkArgument(foundEnding, "Not found ending '0x%s'",
                        Bytes.toHex(STRING_ENDING_BYTE));
        int end = this.buffer.position() - 1;
        //计算由start到end字节长度
        int len = end - start;
        byte[] bytes = new byte[len];
        //数组拷贝
        System.arraycopy(this.array(), start, bytes, 0, len);
        return bytes;
        //一次性连续读，批量数据拷贝
    }

    /**
     * 根据dataType，序列化value
     * @param dataType
     * @param value
     */
    private void writeProperty(DataType dataType, Object value) {
        switch (dataType) {
            case BOOLEAN:
                this.writeVInt(((Boolean) value) ? 1 : 0);
                break;
            case BYTE:
                this.writeVInt((Byte) value);
                break;
            case INT:
                this.writeVInt((Integer) value);
                break;
            case FLOAT:
                this.writeFloat((Float) value);
                break;
            case LONG:
                this.writeVLong((Long) value);
                break;
            case DATE:
                this.writeVLong(((Date) value).getTime());
                break;
            case DOUBLE:
                this.writeDouble((Double) value);
                break;
            case TEXT:
                this.writeString((String) value);
                break;
            case BLOB:
                this.writeBytes((byte[]) value);
                break;
            case UUID:
                UUID uuid = (UUID) value;
                // Generally writeVLong(uuid) can't save space
                this.writeLong(uuid.getMostSignificantBits());
                this.writeLong(uuid.getLeastSignificantBits());
                break;
            default:
                this.writeBytes(KryoUtil.toKryoWithType(value));
                break;
        }
    }

    /**
     * 根据DataType 反序列化value
     * int与long 使用了自己定义的序列化算法
     * @param dataType
     * @return
     */
    private Object readProperty(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                //0000 000x
                return this.readVInt() == 1;
            case BYTE:
                //xxxx xxxx
                return (byte) this.readVInt();
            case INT:
                //自定义Int结构
                return this.readVInt();
            case FLOAT:
                //xxxx xxxx xxxx xxxx
                return this.readFloat();
            case LONG:
                //自定义Long结构
                return this.readVLong();
            case DATE:
                //自定义结构
                return new Date(this.readVLong());
            case DOUBLE:
                //xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx xxxx
                return this.readDouble();
            case TEXT:
                return this.readString();
            case BLOB:
                //length+bytes[]
                return this.readBytes();
            case UUID:
                //64bit+64bit=128bit
                //long+long
                return new UUID(this.readLong(), this.readLong());
            default:
                //length+bytes[] ->尝试解析为UUID
                return KryoUtil.fromKryoWithType(this.readBytes());
        }
    }
}
