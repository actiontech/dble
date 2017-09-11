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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
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
    private final Map<String, ColMeta> columToIndx;
    private final MergeCol[] mergCols;
    private String[] sortColumnsByIndex = null;
    private boolean isMergAvg = false;
    private HavingCols havingCols;
    private UnsafeRow valueKey = null;
    private BufferHolder bufferHolder = null;
    private UnsafeRowWriter unsafeRowWriter = null;
    private final int groupKeyfieldCount;
    private final int valuefieldCount;
    private StructType groupKeySchema;
    private StructType aggBufferSchema;
    private UnsafeRow emptyAggregationBuffer;

    public UnsafeRowGrouper(Map<String, ColMeta> columToIndx, String[] columns, MergeCol[] mergCols, HavingCols havingCols) {
        super();
        assert columns != null;
        assert columToIndx != null;
        assert mergCols != null;
        this.columToIndx = columToIndx;
        String[] columns1 = columns;
        this.mergCols = mergCols;
        this.havingCols = havingCols;
        this.sortColumnsByIndex = columns != null ? toSortColumnsByIndex(columns, columToIndx) : null;
        this.groupKeyfieldCount = columns != null ? columns.length : 0;
        this.valuefieldCount = columToIndx != null ? columToIndx.size() : 0;

        LOGGER.debug("columToIndx :" + (columToIndx != null ? columToIndx.toString() : "null"));

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


        String[] sortColumnsIndexs = new String[map.size()];

        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(
                map.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        Iterator<Map.Entry<String, Integer>> iter = entryList.iterator();
        Map.Entry<String, Integer> tmpEntry = null;

        int index = 0;

        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortColumnsIndexs[index++] = tmpEntry.getKey();
        }

        return sortColumnsIndexs;
    }

    private void initGroupKey() {
        Map<String, ColMeta> groupcolMetaMap = new HashMap<>(this.groupKeyfieldCount);

        UnsafeRow groupKey = new UnsafeRow(this.groupKeyfieldCount);
        bufferHolder = new BufferHolder(groupKey, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyfieldCount);
        bufferHolder.reset();

        ColMeta curColMeta = null;

        for (int i = 0; i < this.groupKeyfieldCount; i++) {
            curColMeta = this.columToIndx.get(sortColumnsByIndex[i].toUpperCase());
            groupcolMetaMap.put(sortColumnsByIndex[i], curColMeta);


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

        groupKeySchema = new StructType(groupcolMetaMap, this.groupKeyfieldCount);
        groupKeySchema.setOrderCols(null);
    }

    private void initEmptyValueKey() {
        emptyAggregationBuffer = new UnsafeRow(this.valuefieldCount);
        bufferHolder = new BufferHolder(emptyAggregationBuffer, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valuefieldCount);
        bufferHolder.reset();

        ColMeta curColMeta = null;
        for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
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
        aggBufferSchema = new StructType(columToIndx, this.valuefieldCount);
        aggBufferSchema.setOrderCols(null);
    }


    public Iterator<UnsafeRow> getResult(@Nonnull UnsafeExternalRowSorter sorter) throws IOException {
        KVIterator<UnsafeRow, UnsafeRow> iter = aggregationMap.iterator();

        if (isMergeAvg() && !isMergAvg) {
            try {
                while (iter.next()) {
                    mergAvg(iter.getValue());
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
            isMergAvg = true;
            processAvgFieldPrecision();
        }
        /**
         * group having
         */
        if (havingCols != null) {
            filterHaving(sorter);
        } else {

            /**
             * KVIterator<K,V> ==>Iterator<V>
             */
            insertValue(sorter);
        }
        return sorter.sort();
    }

    private void processAvgFieldPrecision() {
        for (Map.Entry<String, ColMeta> entry : columToIndx.entrySet()) {
            if (isAvgField(entry.getKey())) { // AVG's Precision is sum's +4 , HALF_UP
                entry.getValue().setDecimals(entry.getValue().getDecimals() + 4);
            }
        }
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

        UnsafeRow value = new UnsafeRow(this.valuefieldCount);
        bufferHolder = new BufferHolder(value, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valuefieldCount);
        bufferHolder.reset();
        ColMeta curColMeta = null;

        for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
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

    private void filterHaving(@Nonnull UnsafeExternalRowSorter sorter) {

        if (havingCols.getColMeta() == null || aggregationMap == null) {
            return;
        }
        KVIterator<UnsafeRow, UnsafeRow> it = aggregationMap.iterator();
        byte[] right = havingCols.getRight().getBytes(StandardCharsets.UTF_8);
        int index = havingCols.getColMeta().getColIndex();
        try {
            while (it.next()) {
                UnsafeRow row = getAllBinaryRow(it.getValue());
                switch (havingCols.getOperator()) {
                    case "=":
                        if (!eq(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    case ">":
                        if (!gt(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    case "<":
                        if (!lt(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    case ">=":
                        if (!gt(row.getBinary(index), right) && eq(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    case "<=":
                        if (!lt(row.getBinary(index), right) && eq(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    case "!=":
                        if (!neq(row.getBinary(index), right)) {
                            sorter.insertRow(row);
                        }
                        break;
                    default:
                        break;
                }
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
            key = new UnsafeRow(this.groupKeyfieldCount + 1);
            bufferHolder = new BufferHolder(key, 0);
            unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyfieldCount + 1);
            bufferHolder.reset();
            unsafeRowWriter.write(0, "same".getBytes());
            key.setTotalSize(bufferHolder.totalSize());
            return key;
        }


        key = new UnsafeRow(this.groupKeyfieldCount);
        bufferHolder = new BufferHolder(key, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.groupKeyfieldCount);
        bufferHolder.reset();


        ColMeta curColMeta = null;
        for (int i = 0; i < this.groupKeyfieldCount; i++) {
            curColMeta = this.columToIndx.get(sortColumnsByIndex[i].toUpperCase());
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

        UnsafeRow value = new UnsafeRow(this.valuefieldCount);
        bufferHolder = new BufferHolder(value, 0);
        unsafeRowWriter = new UnsafeRowWriter(bufferHolder, this.valuefieldCount);
        bufferHolder.reset();
        ColMeta curColMeta = null;
        for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
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

        if (aggregationMap.find(key)) {
            UnsafeRow rs = aggregationMap.getAggregationBuffer(key);
            aggregateRow(rs, value);
        } else {
            aggregationMap.put(key, value);
        }

        return;
    }


    private boolean isMergeAvg() {

        if (mergCols == null) {
            return false;
        }

        for (MergeCol merg : mergCols) {
            if (merg.mergeType == MergeCol.MERGE_AVG) {
                return true;
            }
        }
        return false;
    }

    private void aggregateRow(UnsafeRow toRow, UnsafeRow newRow) throws UnsupportedEncodingException {
        if (mergCols == null) {
            return;
        }

        for (MergeCol merg : mergCols) {
            if (merg.mergeType != MergeCol.MERGE_AVG) {
                byte[] result = null;
                byte[] left = null;
                byte[] right = null;
                int type = merg.colMeta.getColType();
                int index = merg.colMeta.getColIndex();
                left = unsafeRow2Bytes(toRow, merg);
                right = unsafeRow2Bytes(newRow, merg);
                result = mertFields(left, right, type, merg.mergeType);

                if (result != null) {
                    switch (type) {
                        case ColMeta.COL_TYPE_BIT:
                            toRow.setByte(index, result[0]);
                            // fallthrough
                        case ColMeta.COL_TYPE_INT:
                        case ColMeta.COL_TYPE_LONG:
                        case ColMeta.COL_TYPE_INT24:
                            toRow.setInt(index, BytesTools.getInt(result));
                            break;
                        case ColMeta.COL_TYPE_SHORT:
                            toRow.setShort(index, BytesTools.getShort(result));
                            break;
                        case ColMeta.COL_TYPE_LONGLONG:
                            toRow.setLong(index, BytesTools.getLong(result));
                            break;
                        case ColMeta.COL_TYPE_FLOAT:
                            toRow.setFloat(index, BytesTools.getFloat(result));
                            break;
                        case ColMeta.COL_TYPE_DOUBLE:
                            toRow.setDouble(index, BytesTools.getDouble(result));
                            break;
                        case ColMeta.COL_TYPE_NEWDECIMAL:
                            //toRow.setDouble(index,BytesTools.getDouble(result));
                            toRow.updateDecimal(index, new BigDecimal(new String(result)));
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    private byte[] unsafeRow2Bytes(UnsafeRow row, MergeCol merg) throws UnsupportedEncodingException {
        int index = merg.colMeta.getColIndex();
        byte[] result = null;
        if (row.isNullAt(index)) {
            return null;
        }
        int type = merg.colMeta.getColType();
        switch (type) {
            case ColMeta.COL_TYPE_INT:
            case ColMeta.COL_TYPE_LONG:
            case ColMeta.COL_TYPE_INT24:
                result = BytesTools.int2Bytes(row.getInt(index));
                break;
            case ColMeta.COL_TYPE_SHORT:
                result = BytesTools.short2Bytes(row.getShort(index));
                break;
            case ColMeta.COL_TYPE_LONGLONG:
                result = BytesTools.long2Bytes(row.getLong(index));
                break;
            case ColMeta.COL_TYPE_FLOAT:
                result = BytesTools.float2Bytes(row.getFloat(index));
                break;
            case ColMeta.COL_TYPE_DOUBLE:
                result = BytesTools.double2Bytes(row.getDouble(index));
                break;
            case ColMeta.COL_TYPE_NEWDECIMAL:
                int scale = merg.colMeta.getDecimals();
                BigDecimal decimalLeft = row.getDecimal(index, scale);
                result = decimalLeft == null ? null : decimalLeft.toString().getBytes();
                break;
            default:
                break;
        }
        return result;
    }

    private void mergAvg(UnsafeRow toRow) throws UnsupportedEncodingException {

        if (mergCols == null) {
            return;
        }

        for (MergeCol merg : mergCols) {
            if (merg.mergeType == MergeCol.MERGE_AVG) {
                byte[] result = null;
                byte[] avgSum = null;
                byte[] avgCount = null;

                int type = merg.colMeta.getColType();
                int avgSumIndex = merg.colMeta.getAvgSumIndex();
                int avgCountIndex = merg.colMeta.getAvgCountIndex();

                switch (type) {
                    case ColMeta.COL_TYPE_BIT:
                        avgSum = BytesTools.toBytes(toRow.getByte(avgSumIndex));
                        avgCount = BytesTools.toBytes(toRow.getLong(avgCountIndex));
                        break;
                    case ColMeta.COL_TYPE_INT:
                    case ColMeta.COL_TYPE_LONG:
                    case ColMeta.COL_TYPE_INT24:
                        avgSum = BytesTools.int2Bytes(toRow.getInt(avgSumIndex));
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
                        break;
                    case ColMeta.COL_TYPE_SHORT:
                        avgSum = BytesTools.short2Bytes(toRow.getShort(avgSumIndex));
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
                        break;

                    case ColMeta.COL_TYPE_LONGLONG:
                        avgSum = BytesTools.long2Bytes(toRow.getLong(avgSumIndex));
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));

                        break;
                    case ColMeta.COL_TYPE_FLOAT:
                        avgSum = BytesTools.float2Bytes(toRow.getFloat(avgSumIndex));
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));

                        break;
                    case ColMeta.COL_TYPE_DOUBLE:
                        avgSum = BytesTools.double2Bytes(toRow.getDouble(avgSumIndex));
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
                        break;
                    case ColMeta.COL_TYPE_NEWDECIMAL:
                        //avgSum = BytesTools.double2Bytes(toRow.getDouble(avgSumIndex));
                        //avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
                        int scale = merg.colMeta.getDecimals();
                        BigDecimal sumDecimal = toRow.getDecimal(avgSumIndex, scale);
                        avgSum = sumDecimal == null ? null : sumDecimal.toString().getBytes();
                        avgCount = BytesTools.long2Bytes(toRow.getLong(avgCountIndex));
                        break;
                    default:
                        break;
                }

                result = mertFields(avgSum, avgCount, merg.colMeta.getColType(), merg.mergeType);

                if (result != null) {
                    switch (type) {
                        case ColMeta.COL_TYPE_BIT:
                            toRow.setByte(avgSumIndex, result[0]);
                            break;
                        case ColMeta.COL_TYPE_INT:
                        case ColMeta.COL_TYPE_LONG:
                        case ColMeta.COL_TYPE_INT24:
                            toRow.setInt(avgSumIndex, BytesTools.getInt(result));
                            break;
                        case ColMeta.COL_TYPE_SHORT:
                            toRow.setShort(avgSumIndex, BytesTools.getShort(result));
                            break;
                        case ColMeta.COL_TYPE_LONGLONG:
                            toRow.setLong(avgSumIndex, BytesTools.getLong(result));
                            break;
                        case ColMeta.COL_TYPE_FLOAT:
                            toRow.setFloat(avgSumIndex, BytesTools.getFloat(result));
                            break;
                        case ColMeta.COL_TYPE_DOUBLE:
                            toRow.setDouble(avgSumIndex, ByteUtil.getDouble(result));
                            break;
                        case ColMeta.COL_TYPE_NEWDECIMAL:
                            //toRow.setDouble(avgSumIndex,ByteUtil.getDouble(result));
                            toRow.updateDecimal(avgSumIndex, new BigDecimal(new String(result)));
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    private byte[] mertFields(byte[] bs, byte[] bs2, int colType, int mergeType) throws UnsupportedEncodingException {

        if (bs2 == null || bs2.length == 0) {
            return bs;
        } else if (bs == null || bs.length == 0) {
            return bs2;
        }

        switch (mergeType) {
            case MergeCol.MERGE_SUM:
                if (colType == ColMeta.COL_TYPE_DOUBLE ||
                        colType == ColMeta.COL_TYPE_FLOAT) {
                    double value = BytesTools.getDouble(bs) +
                            BytesTools.getDouble(bs2);

                    return BytesTools.double2Bytes(value);
                } else if (colType == ColMeta.COL_TYPE_NEWDECIMAL ||
                        colType == ColMeta.COL_TYPE_DECIMAL) {
                    BigDecimal decimal = new BigDecimal(new String(bs));
                    decimal = decimal.add(new BigDecimal(new String(bs2)));
                    return decimal.toString().getBytes();
                }
                return null;

            case MergeCol.MERGE_COUNT: {
                long s1 = BytesTools.getLong(bs);
                long s2 = BytesTools.getLong(bs2);
                long total = s1 + s2;
                return BytesTools.long2Bytes(total);
            }

            case MergeCol.MERGE_MAX: {
                int compare = ByteUtil.compareNumberByte(bs, bs2);
                return (compare > 0) ? bs : bs2;
            }

            case MergeCol.MERGE_MIN: {
                int compare = ByteUtil.compareNumberByte(bs, bs2);
                return (compare > 0) ? bs2 : bs;

            }
            case MergeCol.MERGE_AVG: {
                /**
                 * count(*)
                 */
                long count = BytesTools.getLong(bs2);
                if (colType == ColMeta.COL_TYPE_DOUBLE || colType == ColMeta.COL_TYPE_FLOAT) {
                    /**
                     * sum(*)
                     */
                    double sum = BytesTools.getDouble(bs);
                    double value = sum / count;
                    return BytesTools.double2Bytes(value);
                } else if (colType == ColMeta.COL_TYPE_NEWDECIMAL || colType == ColMeta.COL_TYPE_DECIMAL) {
                    BigDecimal sum = new BigDecimal(new String(bs));
                    // AVG's Precision is sum's +4 , HALF_UP
                    BigDecimal avg = sum.divide(new BigDecimal(count), sum.scale() + 4, RoundingMode.HALF_UP);
                    return avg.toString().getBytes();
                }
                return null;
            }
            default:
                return null;
        }
    }

    public void free() {
        if (aggregationMap != null)
            aggregationMap.free();
    }
}
