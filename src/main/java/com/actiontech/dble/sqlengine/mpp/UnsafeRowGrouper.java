/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.memory.SeverMemory;
import com.actiontech.dble.memory.unsafe.KVIterator;
import com.actiontech.dble.memory.unsafe.map.UnsafeFixedWidthAggregationMap;
import com.actiontech.dble.memory.unsafe.memory.mm.DataNodeMemoryManager;
import com.actiontech.dble.memory.unsafe.memory.mm.MemoryManager;
import com.actiontech.dble.memory.unsafe.row.BufferHolder;
import com.actiontech.dble.memory.unsafe.row.StructType;
import com.actiontech.dble.memory.unsafe.row.UnsafeRow;
import com.actiontech.dble.memory.unsafe.row.UnsafeRowWriter;
import com.actiontech.dble.memory.unsafe.utils.BytesTools;
import com.actiontech.dble.memory.unsafe.utils.ServerPropertyConf;
import com.actiontech.dble.memory.unsafe.utils.sort.UnsafeExternalRowSorter;
import com.actiontech.dble.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zagnix on 2016/6/26.
 * <p>
 * implement group function select a,count(*),sum(*) from A group by a
 */
public class UnsafeRowGrouper {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnsafeRowGrouper.class);

    private UnsafeFixedWidthAggregationMap aggregationMap = null;
    private final Map<String, ColMeta> columnToIndexes;
    private String[] sortColumnsByIndex = null;
    private UnsafeRow valueKey = null;
    private BufferHolder bufferHolder = null;
    private UnsafeRowWriter unsafeRowWriter = null;
    private final int groupKeyFieldCount;
    private final int valueFieldCount;
    private StructType groupKeySchema;
    private StructType aggBufferSchema;
    private UnsafeRow emptyAggregationBuffer;

    public UnsafeRowGrouper(Map<String, ColMeta> columnToIndexes, String[] columns) {
        super();
        assert columns != null;
        assert columnToIndexes != null;
        this.columnToIndexes = columnToIndexes;
        this.sortColumnsByIndex = columns != null ? toSortColumnsByIndex(columns, columnToIndexes) : null;
        this.groupKeyFieldCount = columns != null ? columns.length : 0;
        this.valueFieldCount = columnToIndexes != null ? columnToIndexes.size() : 0;

        LOGGER.debug("columnToIndex :" + (columnToIndexes != null ? columnToIndexes.toString() : "null"));

        SeverMemory serverMemory = DbleServer.getInstance().getServerMemory();
        MemoryManager memoryManager = serverMemory.getResultMergeMemoryManager();
        ServerPropertyConf conf = serverMemory.getConf();

        initGroupKey();
        initEmptyValueKey();

        DataNodeMemoryManager dataNodeMemoryManager =
                new DataNodeMemoryManager(memoryManager, Thread.currentThread().getId());

        aggregationMap = new UnsafeFixedWidthAggregationMap(
                emptyAggregationBuffer,
                aggBufferSchema,
                groupKeySchema,
                dataNodeMemoryManager,
                2 * 1024,
                conf.getSizeAsBytes("server.buffer.pageSize", "1m"),
                false);
    }

    private String[] toSortColumnsByIndex(String[] columns, Map<String, ColMeta> colMetaMap) {

        Map<String, Integer> map = new HashMap<>();

        ColMeta curColMeta;
        for (String column : columns) {
            curColMeta = colMetaMap.get(column.toUpperCase());
            if (curColMeta == null) {
                throw new IllegalArgumentException(
                        "all columns in group by clause should be in the selected column list.!" + column);
            }
            map.put(column, curColMeta.getColIndex());
        }


        String[] sortColumnsIndexes = new String[map.size()];

        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(
                map.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        Iterator<Map.Entry<String, Integer>> iterator = entryList.iterator();
        Map.Entry<String, Integer> tmpEntry = null;

        int index = 0;

        while (iterator.hasNext()) {
            tmpEntry = iterator.next();
            sortColumnsIndexes[index++] = tmpEntry.getKey();
        }

        return sortColumnsIndexes;
    }

    private void initGroupKey() {
        Map<String, ColMeta> groupColMetaMap = new HashMap<>(this.groupKeyFieldCount);

        UnsafeRow groupKey = new UnsafeRow(this.groupKeyFieldCount);
        bufferHolder = new BufferHolder(groupKey, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyFieldCount);
        bufferHolder.reset();

        ColMeta curColMeta = null;

        for (int i = 0; i < this.groupKeyFieldCount; i++) {
            curColMeta = this.columnToIndexes.get(sortColumnsByIndex[i].toUpperCase());
            groupColMetaMap.put(sortColumnsByIndex[i], curColMeta);


            switch (curColMeta.getColType()) {
                case ColMeta.COL_TYPE_BIT:
                    groupKey.setByte(i, (byte) 0);
                    break;
                case ColMeta.COL_TYPE_INT:
                case ColMeta.COL_TYPE_INT24:
                case ColMeta.COL_TYPE_LONG:
                    groupKey.setInt(i, 0);
                    break;
                case ColMeta.COL_TYPE_SHORT:
                    groupKey.setShort(i, (short) 0);
                    break;
                case ColMeta.COL_TYPE_FLOAT:
                    groupKey.setFloat(i, 0);
                    break;
                case ColMeta.COL_TYPE_DOUBLE:
                    groupKey.setDouble(i, 0);
                    break;
                case ColMeta.COL_TYPE_NEWDECIMAL:
                    //groupKey.setDouble(i, 0);
                    unsafeRowWriter.write(i, new BigDecimal(0L));
                    break;
                case ColMeta.COL_TYPE_LONGLONG:
                    groupKey.setLong(i, 0);
                    break;
                default:
                    unsafeRowWriter.write(i, "init".getBytes());
                    break;
            }

        }
        groupKey.setTotalSize(bufferHolder.totalSize());

        groupKeySchema = new StructType(groupColMetaMap, this.groupKeyFieldCount);
    }

    private void initEmptyValueKey() {
        emptyAggregationBuffer = new UnsafeRow(this.valueFieldCount);
        bufferHolder = new BufferHolder(emptyAggregationBuffer, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valueFieldCount);
        bufferHolder.reset();

        ColMeta curColMeta = null;
        for (Map.Entry<String, ColMeta> fieldEntry : columnToIndexes.entrySet()) {
            curColMeta = fieldEntry.getValue();

            switch (curColMeta.getColType()) {
                case ColMeta.COL_TYPE_BIT:
                    emptyAggregationBuffer.setByte(curColMeta.getColIndex(), (byte) 0);
                    break;
                case ColMeta.COL_TYPE_INT:
                case ColMeta.COL_TYPE_INT24:
                case ColMeta.COL_TYPE_LONG:
                    emptyAggregationBuffer.setInt(curColMeta.getColIndex(), 0);
                    break;
                case ColMeta.COL_TYPE_SHORT:
                    emptyAggregationBuffer.setShort(curColMeta.getColIndex(), (short) 0);
                    break;
                case ColMeta.COL_TYPE_LONGLONG:
                    emptyAggregationBuffer.setLong(curColMeta.getColIndex(), 0);
                    break;
                case ColMeta.COL_TYPE_FLOAT:
                    emptyAggregationBuffer.setFloat(curColMeta.getColIndex(), 0);
                    break;
                case ColMeta.COL_TYPE_DOUBLE:
                    emptyAggregationBuffer.setDouble(curColMeta.getColIndex(), 0);
                    break;
                case ColMeta.COL_TYPE_NEWDECIMAL:
                    //emptyAggregationBuffer.setDouble(curColMeta.colIndex, 0);
                    unsafeRowWriter.write(curColMeta.getColIndex(), new BigDecimal(0L));
                    break;
                default:
                    unsafeRowWriter.write(curColMeta.getColIndex(), "init".getBytes());
                    break;
            }

        }

        emptyAggregationBuffer.setTotalSize(bufferHolder.totalSize());
        aggBufferSchema = new StructType(columnToIndexes, this.valueFieldCount);
    }


    public Iterator<UnsafeRow> getResult(@Nonnull UnsafeExternalRowSorter sorter) throws IOException {
        KVIterator<UnsafeRow, UnsafeRow> iterator = aggregationMap.iterator();
        /**
         * group having
         */
        insertValue(sorter);
        return sorter.sort();
    }


    /**
     * is Avg Field
     *
     * @param columnName
     * @return
     */
    private boolean isAvgField(String columnName) {
        Pattern pattern = Pattern.compile("AVG([1-9]\\d*|0)SUM");
        Matcher matcher = pattern.matcher(columnName);
        return matcher.find();
    }


    public UnsafeRow getAllBinaryRow(UnsafeRow row) throws UnsupportedEncodingException {

        UnsafeRow value = new UnsafeRow(this.valueFieldCount);
        bufferHolder = new BufferHolder(value, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valueFieldCount);
        bufferHolder.reset();
        ColMeta curColMeta = null;

        for (Map.Entry<String, ColMeta> fieldEntry : columnToIndexes.entrySet()) {
            curColMeta = fieldEntry.getValue();

            if (!row.isNullAt(curColMeta.getColIndex())) {
                switch (curColMeta.getColType()) {
                    case ColMeta.COL_TYPE_BIT:
                        unsafeRowWriter.write(curColMeta.getColIndex(), row.getByte(curColMeta.getColIndex()));
                        break;
                    case ColMeta.COL_TYPE_INT:
                    case ColMeta.COL_TYPE_LONG:
                    case ColMeta.COL_TYPE_INT24:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                BytesTools.int2Bytes(row.getInt(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_SHORT:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                BytesTools.short2Bytes(row.getShort(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_LONGLONG:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                BytesTools.long2Bytes(row.getLong(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_FLOAT:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                BytesTools.float2Bytes(row.getFloat(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_DOUBLE:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                BytesTools.double2Bytes(row.getDouble(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_NEWDECIMAL:
                        int scale = curColMeta.getDecimals();
                        BigDecimal decimalVal = row.getDecimal(curColMeta.getColIndex(), scale);
                        unsafeRowWriter.write(curColMeta.getColIndex(), decimalVal.toString().getBytes());
                        break;
                    default:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                row.getBinary(curColMeta.getColIndex()));
                        break;
                }
            } else {
                unsafeRowWriter.setNullAt(curColMeta.getColIndex());
            }
        }

        value.setTotalSize(bufferHolder.totalSize());
        return value;
    }

    private void insertValue(@Nonnull UnsafeExternalRowSorter sorter) {
        KVIterator<UnsafeRow, UnsafeRow> it = aggregationMap.iterator();
        try {
            while (it.next()) {
                UnsafeRow row = getAllBinaryRow(it.getValue());
                sorter.insertRow(row);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }


    private boolean lt(byte[] l, byte[] r) {
        return -1 != ByteUtil.compareNumberByte(l, r);
    }

    private boolean gt(byte[] l, byte[] r) {
        return 1 != ByteUtil.compareNumberByte(l, r);
    }

    private boolean eq(byte[] l, byte[] r) {
        return 0 != ByteUtil.compareNumberByte(l, r);
    }

    private boolean neq(byte[] l, byte[] r) {
        return 0 == ByteUtil.compareNumberByte(l, r);
    }

    private UnsafeRow getGroupKey(UnsafeRow row) throws UnsupportedEncodingException {

        UnsafeRow key = null;
        if (this.sortColumnsByIndex == null) {
            /**
             * no group by key word
             * select count(*) from table;
             */
            key = new UnsafeRow(this.groupKeyFieldCount + 1);
            bufferHolder = new BufferHolder(key, 0);
            unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyFieldCount + 1);
            bufferHolder.reset();
            unsafeRowWriter.write(0, "same".getBytes());
            key.setTotalSize(bufferHolder.totalSize());
            return key;
        }


        key = new UnsafeRow(this.groupKeyFieldCount);
        bufferHolder = new BufferHolder(key, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyFieldCount);
        bufferHolder.reset();


        ColMeta curColMeta = null;
        for (int i = 0; i < this.groupKeyFieldCount; i++) {
            curColMeta = this.columnToIndexes.get(sortColumnsByIndex[i].toUpperCase());
            if (!row.isNullAt(curColMeta.getColIndex())) {
                switch (curColMeta.getColType()) {
                    case ColMeta.COL_TYPE_BIT:
                        key.setByte(i, row.getByte(curColMeta.getColIndex()));
                        // fallthrough
                    case ColMeta.COL_TYPE_INT:
                    case ColMeta.COL_TYPE_LONG:
                    case ColMeta.COL_TYPE_INT24:
                        key.setInt(i,
                                BytesTools.getInt(row.getBinary(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_SHORT:
                        key.setShort(i,
                                BytesTools.getShort(row.getBinary(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_FLOAT:
                        key.setFloat(i,
                                BytesTools.getFloat(row.getBinary(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_DOUBLE:
                        key.setDouble(i,
                                BytesTools.getDouble(row.getBinary(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_NEWDECIMAL:
                        //key.setDouble(i, BytesTools.getDouble(row.getBinary(curColMeta.colIndex)));
                        unsafeRowWriter.write(i,
                                new BigDecimal(new String(row.getBinary(curColMeta.getColIndex()))));
                        break;
                    case ColMeta.COL_TYPE_LONGLONG:
                        key.setLong(i,
                                BytesTools.getLong(row.getBinary(curColMeta.getColIndex())));
                        break;
                    default:
                        unsafeRowWriter.write(i,
                                row.getBinary(curColMeta.getColIndex()));
                        break;
                }
            } else {
                key.setNullAt(i);
            }
        }

        key.setTotalSize(bufferHolder.totalSize());

        return key;
    }

    private UnsafeRow getValue(UnsafeRow row) throws UnsupportedEncodingException {

        UnsafeRow value = new UnsafeRow(this.valueFieldCount);
        bufferHolder = new BufferHolder(value, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valueFieldCount);
        bufferHolder.reset();
        ColMeta curColMeta = null;
        for (Map.Entry<String, ColMeta> fieldEntry : columnToIndexes.entrySet()) {
            curColMeta = fieldEntry.getValue();
            if (!row.isNullAt(curColMeta.getColIndex())) {
                switch (curColMeta.getColType()) {
                    case ColMeta.COL_TYPE_BIT:
                        value.setByte(curColMeta.getColIndex(), row.getByte(curColMeta.getColIndex()));
                        break;
                    case ColMeta.COL_TYPE_INT:
                    case ColMeta.COL_TYPE_LONG:
                    case ColMeta.COL_TYPE_INT24:
                        value.setInt(curColMeta.getColIndex(),
                                BytesTools.getInt(row.getBinary(curColMeta.getColIndex())));

                        break;
                    case ColMeta.COL_TYPE_SHORT:
                        value.setShort(curColMeta.getColIndex(),
                                BytesTools.getShort(row.getBinary(curColMeta.getColIndex())));
                        break;
                    case ColMeta.COL_TYPE_LONGLONG:
                        value.setLong(curColMeta.getColIndex(),
                                BytesTools.getLong(row.getBinary(curColMeta.getColIndex())));


                        break;
                    case ColMeta.COL_TYPE_FLOAT:
                        value.setFloat(curColMeta.getColIndex(),
                                BytesTools.getFloat(row.getBinary(curColMeta.getColIndex())));

                        break;
                    case ColMeta.COL_TYPE_DOUBLE:
                        value.setDouble(curColMeta.getColIndex(), BytesTools.getDouble(row.getBinary(curColMeta.getColIndex())));

                        break;
                    case ColMeta.COL_TYPE_NEWDECIMAL:
                        //value.setDouble(curColMeta.colIndex, BytesTools.getDouble(row.getBinary(curColMeta.colIndex)));
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                new BigDecimal(new String(row.getBinary(curColMeta.getColIndex()))));
                        break;
                    default:
                        unsafeRowWriter.write(curColMeta.getColIndex(),
                                row.getBinary(curColMeta.getColIndex()));
                        break;
                }
            } else {
                switch (curColMeta.getColType()) {
                    case ColMeta.COL_TYPE_NEWDECIMAL:
                        BigDecimal nullDecimal = null;
                        unsafeRowWriter.write(curColMeta.getColIndex(), nullDecimal);
                        break;
                    default:
                        value.setNullAt(curColMeta.getColIndex());
                        break;
                }
            }
        }


        value.setTotalSize(bufferHolder.totalSize());
        return value;
    }

    public void addRow(UnsafeRow rowDataPkg) throws UnsupportedEncodingException {
        UnsafeRow key = getGroupKey(rowDataPkg);
        UnsafeRow value = getValue(rowDataPkg);

        if (!aggregationMap.find(key)) {
            aggregationMap.put(key, value);
        }
    }

    public void free() {
        if (aggregationMap != null)
            aggregationMap.free();
    }
}
