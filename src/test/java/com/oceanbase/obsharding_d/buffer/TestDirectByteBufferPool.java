/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.buffer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDirectByteBufferPool {

    @Test
    public void testAllocate() {
        int pageSize = 1024 * 1024 * 100;
        int allocTimes = 1024;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 8);
        long start = System.currentTimeMillis();
        for (int i = 0; i < allocTimes; i++) {
            //System.out.println("allocate "+i);
            //long start=System.nanoTime();
            int size = (i % 1024) + 1;
            ByteBuffer byteBufer = pool.allocate(size, null);
            ByteBuffer byteBufer2 = pool.allocate(size, null);
            ByteBuffer byteBufer3 = pool.allocate(size, null);
            //System.out.println("alloc "+size+" usage "+(System.nanoTime()-start));
            //start=System.nanoTime();
            pool.recycle(byteBufer);
            pool.recycle(byteBufer2);
            pool.recycle(byteBufer3);
            //System.out.println("recycle usage "+(System.nanoTime()-start));
        }
        long used = (System.currentTimeMillis() - start);
        System.out.println("total used time  " + used + " avg speed " + allocTimes / used);
    }

    @Test
    @Ignore
    public void testAllocateTime() {
        int pageSize = 1024 * 1024 * 10;
        int allocTimes = 20480;
        int size = 4096;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 8);
        long start = System.currentTimeMillis();
        for (int i = 0; i < allocTimes; i++) {
            ByteBuffer byteBuffer = pool.allocate(size, null);
        }
        long used = (System.currentTimeMillis() - start);
        System.out.println("total used time  " + used + " avg speed " + allocTimes / used);
    }

    @Test
    public void testAllocateWithDifferentAddress() {
        int size = 256;
        int pageSize = size * 4;
        int allocTimes = 8;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 2);

        Map<Long, ByteBuffer> buffs = new HashMap<Long, ByteBuffer>(8);
        ByteBuffer byteBuffer = null;
        DirectBuffer directBuffer = null;
        ByteBuffer temp = null;
        long address;
        boolean failure = false;
        for (int i = 0; i < allocTimes; i++) {
            byteBuffer = pool.allocate(size, null);
            //            if (byteBuffer == null) {
            //                Assert.fail("Should have enough memory");
            //            }
            directBuffer = (DirectBuffer) byteBuffer;
            address = directBuffer.address();
            System.out.println(address);
            temp = buffs.get(address);
            buffs.put(address, byteBuffer);
            if (null != temp) {
                failure = true;
                break;
            }
        }

        for (ByteBuffer buff : buffs.values()) {
            pool.recycle(buff);
        }

        if (failure == true) {
            Assert.fail("Allocate with same address");
        }
    }

    @Test
    public void testAllocateNullWhenOutOfMemory() {
        int size = 256;
        int pageSize = size * 4;
        int allocTimes = 9;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 2);

        ByteBuffer byteBuffer = null;
        List<ByteBuffer> buffs = new ArrayList<ByteBuffer>();
        int i = 0;
        for (; i < allocTimes; i++) {
            byteBuffer = pool.allocate(size, null);
            if (byteBuffer == null || !(byteBuffer instanceof DirectBuffer)) {
                break;
            }
            buffs.add(byteBuffer);
        }
        for (ByteBuffer buff : buffs) {
            pool.recycle(buff);
        }

        Assert.assertEquals("Should out of memory when i = " + 8, i, 8);
    }

    @Test
    public void testAllocateSign() {
        int size = 256;
        int pageSize = size * 4;
        int allocTimes = 9;
        DirectByteBufferPool pool = new DirectByteBufferPool(pageSize, (short) 256, (short) 2);

        ByteBuffer byteBuffer = null;
        List<ByteBuffer> buffs = new ArrayList<ByteBuffer>();
        int i = 0;
        for (; i < allocTimes; i++) {
            byteBuffer = pool.allocate(size, null);
            if (byteBuffer == null || !(byteBuffer instanceof DirectBuffer)) {
                break;
            }
            buffs.add(byteBuffer);
        }
        for (ByteBuffer buff : buffs) {
            pool.recycle(buff);
        }

        Assert.assertEquals("Should out of memory when i = " + 8, i, 8);
    }


}
