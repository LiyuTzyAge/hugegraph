package com.baidu.hugegraph.my.core;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

/**
 *  @author: liyu04
 *  @date: 2020/4/22
 *  @version: V1.0
 *
 * @Description:
 */
public class BytesBufferTester
{

    public static byte[] formatColumnName(Id id, HugeKeys key)
    {

        //1 + id.length()=Megic+id
        //1 = code
        int size = 1 + id.length() + 1;
        BytesBuffer buffer = BytesBuffer.allocate(size);
        buffer.writeId(id);
        buffer.write(key.code());
        return buffer.bytes();
    }

    @Test
    public void formatColumnName()
    {
        Id id = IdGenerator.of(1);
        byte[] bytes = formatColumnName(id, HugeKeys.PROPERTIES);
        for (byte b : bytes) {
            System.out.println(Integer.toHexString(b));
        }

    }

    @Test
    public void writeId()
    {
        Id id = IdGenerator.of("1:saturn");
        BytesBuffer byteBuffer = new BytesBuffer(3);
        BytesBuffer bytesBuffer = byteBuffer.writeId(id);
        printHex(byteBuffer.bytes());
    }

    public static void printHex(byte[] array)
    {
        System.out.println();
        System.out.println(Arrays.toString(array));
        for (byte b : array) {
            System.out.println(Integer.toHexString(b));
        }
    }

    @Test
    public void bytesBuffer()
    {
        BytesBuffer byteBuffer = new BytesBuffer(3);
        byteBuffer.writeNumberTest(1);
        byteBuffer.write(3);
        byte[] array = byteBuffer.array();
        printHex(array);
    }


    @Test
    public void newBackendEntry()
    {
        //\x861:pluto -> 0000 1000 0110 0001:pluto ->byte[]{8,97}
        Id id = IdGenerator.of("1:saturn");
        BinarySerializer bs = new BinarySerializer();
        BinaryBackendEntry be = bs.newBackendEntry(HugeType.VERTEX, id);
        System.out.println(be.id().origin());
        printHex(be.id().asBytes());
        System.out.println(Bytes.toHex(be.id().asBytes()));

    }
}
