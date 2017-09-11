/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.sort;

import com.actiontech.dble.memory.SeverMemory;
import com.actiontech.dble.memory.unsafe.memory.mm.DataNodeMemoryManager;
import com.actiontech.dble.memory.unsafe.memory.mm.MemoryManager;
import com.actiontech.dble.memory.unsafe.row.BufferHolder;
import com.actiontech.dble.memory.unsafe.row.StructType;
import com.actiontech.dble.memory.unsafe.row.UnsafeRow;
import com.actiontech.dble.memory.unsafe.row.UnsafeRowWriter;
import com.actiontech.dble.memory.unsafe.utils.ServerPropertyConf;
import com.actiontech.dble.memory.unsafe.utils.sort.PrefixComparator;
import com.actiontech.dble.memory.unsafe.utils.sort.PrefixComparators;
import com.actiontech.dble.memory.unsafe.utils.sort.RowPrefixComputer;
import com.actiontech.dble.memory.unsafe.utils.sort.UnsafeExternalRowSorter;
import com.actiontech.dble.sqlengine.mpp.ColMeta;
import com.actiontech.dble.sqlengine.mpp.OrderCol;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by zagnix on 2016/6/19.
 */
public class UnsafeExternalRowSorterTest {

    private static final int TEST_SIZE = 100000;
    public static final Logger LOGGER = LoggerFactory.getLogger(UnsafeExternalRowSorterTest.class);

    /**
     * test type LONG,INT,SHORT,Float,Double,String,Binary
     */
    @Test
    public void testUnsafeExternalRowSorter() throws NoSuchFieldException, IllegalAccessException, IOException {
        SeverMemory severMemory = new SeverMemory();
        MemoryManager memoryManager = severMemory.getResultMergeMemoryManager();
        ServerPropertyConf conf = severMemory.getConf();
        DataNodeMemoryManager dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,
                Thread.currentThread().getId());

        int fieldCount = 3;
        ColMeta colMeta = null;
        Map<String, ColMeta> colMetaMap = new HashMap<String, ColMeta>(fieldCount);
        colMeta = new ColMeta(0, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("id", colMeta);
        colMeta = new ColMeta(1, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("name", colMeta);
        colMeta = new ColMeta(2, ColMeta.COL_TYPE_STRING);
        colMetaMap.put("age", colMeta);


        OrderCol[] orderCols = new OrderCol[1];
        OrderCol orderCol = new OrderCol(colMetaMap.get("id"),
                OrderCol.COL_ORDER_TYPE_ASC);
        orderCols[0] = orderCol;
        /**
         * 2 .PrefixComputer
         */
        StructType schema = new StructType(colMetaMap, fieldCount);
        schema.setOrderCols(orderCols);

        UnsafeExternalRowSorter.PrefixComputer prefixComputer =
                new RowPrefixComputer(schema);


        final PrefixComparator prefixComparator = PrefixComparators.LONG;

        UnsafeExternalRowSorter sorter =
                new UnsafeExternalRowSorter(dataNodeMemoryManager,
                        severMemory,
                        schema,
                        prefixComparator,
                        prefixComputer,
                        conf.getSizeAsBytes("server.buffer.pageSize", "1m"),
                        true,
                        true);

        UnsafeRow unsafeRow;
        BufferHolder bufferHolder;
        UnsafeRowWriter unsafeRowWriter;
        String line = "testUnsafeRow";
        // List<Float> floats = new ArrayList<Float>();
        List<Long> longs = new ArrayList<Long>();
        final Random rand = new Random(42);
        for (int i = 0; i < TEST_SIZE; i++) {
            unsafeRow = new UnsafeRow(3);
            bufferHolder = new BufferHolder(unsafeRow);
            unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 3);
            bufferHolder.reset();

            String key = getRandomString(rand.nextInt(300) + 100);

            //long v = rand.nextLong();
            // longs.add(v);
            unsafeRowWriter.write(0, key.getBytes());
            // unsafeRowWriter.write(0, BytesTools.toBytes(v));
            unsafeRowWriter.write(1, line.getBytes());
            unsafeRowWriter.write(2, ("35" + 1).getBytes());

            unsafeRow.setTotalSize(bufferHolder.totalSize());
            sorter.insertRow(unsafeRow);
        }

        Iterator<UnsafeRow> iter = sorter.sort();
/*
         float [] com = new float[floats.size()];
        for (int i = 0; i <floats.size() ; i++) {
                com[i] = floats.get(i);
        }
        Arrays.sort(com);


        long[] com = new long[longs.size()];
        for (int i = 0; i < longs.size() ; i++) {
            com[i] = longs.get(i);
        }

        Arrays.sort(com);
        */
        UnsafeRow row = null;
        int indexprint = 0;
        while (iter.hasNext()) {
            row = iter.next();

            // LOGGER.error(indexprint + "    " +  row.getUTF8String(0));
            //Assert.assertEquals(com[indexprint],
            //        BytesTools.toLong(row.getBinary(0)));
            // Double c = Double.parseDouble(String.valueOf(com[indexprint])) ;
            // Double c1 = Double.parseDouble(String.valueOf(BytesTools.toFloat(row.getBinary(0)))) ;
            //  Assert.assertEquals(0,c.compareTo(c1));

            indexprint++;
        }
        Assert.assertEquals(TEST_SIZE, indexprint);
    }

    public static String getRandomString(int length) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }
}
