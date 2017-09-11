/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeQueryHandler;
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
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * Created by zagnix on 2016/6/21.
 */
public class DataNodeMergeManager extends AbstractDataNodeMerge {

    private static final Logger LOGGER = Logger.getLogger(DataNodeMergeManager.class);


    /**
     * global sorter
     */
    private UnsafeExternalRowSorter globalSorter = null;
    /**
     * UnsafeRowGrouper
     */
    private UnsafeRowGrouper unsafeRowGrouper = null;

    /**
     * global merge sorter
     */
    private UnsafeExternalRowSorter globalMergeResult = null;

    /**
     * the context of sorter
     */
    private final SeverMemory serverMemory;
    private final MemoryManager memoryManager;
    private final ServerPropertyConf conf;


    public DataNodeMergeManager(MultiNodeQueryHandler handler, RouteResultset rrs) {
        super(handler, rrs);
        this.serverMemory = DbleServer.getInstance().getServerMemory();
        this.memoryManager = serverMemory.getResultMergeMemoryManager();
        this.conf = serverMemory.getConf();
    }


    public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldSize) throws IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("field metadata keys:" + columToIndx.keySet());
            LOGGER.debug("field metadata values:" + columToIndx.values());
        }

        OrderCol[] orderCols = null;
        StructType schema = null;
        UnsafeExternalRowSorter.PrefixComputer prefixComputer = null;
        PrefixComparator prefixComparator = null;


        DataNodeMemoryManager dataNodeMemoryManager = null;

        this.fieldCount = fieldSize;

        if (rrs.getHavingCols() != null) {
            ColMeta colMeta = columToIndx.get(rrs.getHavingCols().getLeft().toUpperCase());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("getHavingCols:" + rrs.getHavingCols().toString());
            }
            if (colMeta != null) {
                rrs.getHavingCols().setColMeta(colMeta);
            }
        }

        if (rrs.isHasAggrColumn()) {
            List<MergeCol> mergCols = new LinkedList<>();
            Map<String, Integer> mergeColsMap = rrs.getMergeCols();

            if (mergeColsMap != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("isHasAggrColumn:" + mergeColsMap.toString());
                }
                for (Map.Entry<String, Integer> mergEntry : mergeColsMap.entrySet()) {
                    String colName = mergEntry.getKey().toUpperCase();
                    int type = mergEntry.getValue();
                    if (MergeCol.MERGE_AVG == type) {
                        ColMeta sumColMeta = columToIndx.get(colName + "SUM");
                        ColMeta countColMeta = columToIndx.get(colName + "COUNT");
                        if (sumColMeta != null && countColMeta != null) {
                            ColMeta colMeta = new ColMeta(sumColMeta.getColIndex(),
                                    countColMeta.getColIndex(),
                                    sumColMeta.getColType());
                            mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
                        }
                    } else {
                        ColMeta colMeta = columToIndx.get(colName);
                        mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
                    }
                }
            }

            // add no alias merg column
            for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
                String colName = fieldEntry.getKey();
                int result = MergeCol.tryParseAggCol(colName);
                if (result != MergeCol.MERGE_UNSUPPORT && result != MergeCol.MERGE_NOMERGE) {
                    mergCols.add(new MergeCol(fieldEntry.getValue(), result));
                }
            }

            /**
             * Group
             */
            unsafeRowGrouper = new UnsafeRowGrouper(columToIndx, rrs.getGroupByCols(),
                    mergCols.toArray(new MergeCol[mergCols.size()]),
                    rrs.getHavingCols());
        }


        if (rrs.getOrderByCols() != null) {
            LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
            orderCols = new OrderCol[orders.size()];
            int i = 0;
            for (Map.Entry<String, Integer> entry : orders.entrySet()) {
                String key = StringUtil.removeBackQuote(entry.getKey().toUpperCase());
                ColMeta colMeta = columToIndx.get(key);
                if (colMeta == null) {
                    throw new IllegalArgumentException(
                            "all columns in order by clause should be in the selected column list!" + entry.getKey());
                }
                orderCols[i++] = new OrderCol(colMeta, entry.getValue());
            }

            schema = new StructType(columToIndx, fieldSize);
            schema.setOrderCols(orderCols);

            prefixComputer = new RowPrefixComputer(schema);

            prefixComparator = getPrefixComparator(orderCols);

            dataNodeMemoryManager =
                    new DataNodeMemoryManager(memoryManager, Thread.currentThread().getId());

            /**
             * default sorter, just store the data in fact
             */
            globalSorter = new UnsafeExternalRowSorter(
                    dataNodeMemoryManager,
                    serverMemory,
                    schema,
                    prefixComparator, prefixComputer,
                    conf.getSizeAsBytes("server.buffer.pageSize", "1m"),
                    false,
                    true);
        }


        /**
         * 1.schema
         */

        schema = new StructType(columToIndx, fieldSize);
        schema.setOrderCols(orderCols);

        /**
         * 2 .PrefixComputer
         */
        prefixComputer = new RowPrefixComputer(schema);

        /**
         * 3 .PrefixComparator ,ASC/DESC and the default is ASC
         */

        prefixComparator = PrefixComparators.LONG;


        dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,
                Thread.currentThread().getId());

        globalMergeResult = new UnsafeExternalRowSorter(
                dataNodeMemoryManager,
                serverMemory,
                schema,
                prefixComparator,
                prefixComputer,
                conf.getSizeAsBytes("server.buffer.pageSize", "1m"),
                false,
                false);
    }

    private PrefixComparator getPrefixComparator(OrderCol[] orderCols) {
        PrefixComparator prefixComparator = null;
        OrderCol firstOrderCol = orderCols[0];
        int orderType = firstOrderCol.getOrderType();
        int colType = firstOrderCol.colMeta.getColType();

        switch (colType) {
            case ColMeta.COL_TYPE_INT:
            case ColMeta.COL_TYPE_LONG:
            case ColMeta.COL_TYPE_INT24:
            case ColMeta.COL_TYPE_SHORT:
            case ColMeta.COL_TYPE_LONGLONG:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ?
                        PrefixComparators.LONG : PrefixComparators.LONG_DESC);
                break;
            case ColMeta.COL_TYPE_FLOAT:
            case ColMeta.COL_TYPE_DOUBLE:
            case ColMeta.COL_TYPE_DECIMAL:
            case ColMeta.COL_TYPE_NEWDECIMAL:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ?
                        PrefixComparators.DOUBLE : PrefixComparators.DOUBLE_DESC);
                break;
            case ColMeta.COL_TYPE_DATE:
            case ColMeta.COL_TYPE_TIMSTAMP:
            case ColMeta.COL_TYPE_TIME:
            case ColMeta.COL_TYPE_YEAR:
            case ColMeta.COL_TYPE_DATETIME:
            case ColMeta.COL_TYPE_NEWDATE:
            case ColMeta.COL_TYPE_BIT:
            case ColMeta.COL_TYPE_VAR_STRING:
            case ColMeta.COL_TYPE_STRING:
            case ColMeta.COL_TYPE_ENUM:
            case ColMeta.COL_TYPE_SET:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ?
                        PrefixComparators.BINARY : PrefixComparators.BINARY_DESC);
                break;
            default:
                prefixComparator = (orderType == OrderCol.COL_ORDER_TYPE_ASC ?
                        PrefixComparators.LONG : PrefixComparators.LONG_DESC);
                break;
        }

        return prefixComparator;
    }

    @Override
    public List<RowDataPacket> getResults(byte[] eof) {
        return null;
    }

    @Override
    public void run() {

        if (!running.compareAndSet(false, true)) {
            return;
        }

        boolean nulpack = false;

        try {
            for (; ; ) {
                final PackWraper pack = packs.poll();

                if (pack == null) {
                    nulpack = true;
                    break;
                }
                if (pack == endFlagPack) {
                    /**
                     * if last date node send row eof packet
                     * means all the data have received
                     */
                    final int warningCount = 0;
                    final EOFPacket eofp = new EOFPacket();
                    final ByteBuffer eof = ByteBuffer.allocate(9);
                    BufferUtil.writeUB3(eof, eofp.calcPacketSize());
                    eof.put(eofp.getPacketId());
                    eof.put(eofp.getFieldCount());
                    BufferUtil.writeUB2(eof, warningCount);
                    BufferUtil.writeUB2(eof, eofp.getStatus());
                    final ServerConnection source = multiQueryHandler.getSession().getSource();
                    final byte[] array = eof.array();


                    Iterator<UnsafeRow> iters = null;


                    if (unsafeRowGrouper != null) {
                        /**
                         * group by need order
                         */
                        if (globalSorter != null) {
                            iters = unsafeRowGrouper.getResult(globalSorter);
                        } else {
                            iters = unsafeRowGrouper.getResult(globalMergeResult);
                        }

                    } else if (globalSorter != null) {

                        iters = globalSorter.sort();

                    } else {

                        iters = globalMergeResult.sort();

                    }

                    if (iters != null)
                        multiQueryHandler.outputMergeResult(source, array, iters);


                    if (unsafeRowGrouper != null) {
                        unsafeRowGrouper.free();
                        unsafeRowGrouper = null;
                    }

                    if (globalSorter != null) {
                        globalSorter.cleanupResources();
                        globalSorter = null;
                    }

                    if (globalMergeResult != null) {
                        globalMergeResult.cleanupResources();
                        globalMergeResult = null;
                    }

                    break;
                }

                UnsafeRow unsafeRow = new UnsafeRow(fieldCount);
                BufferHolder bufferHolder = new BufferHolder(unsafeRow, 0);
                UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder, fieldCount);
                bufferHolder.reset();

                /**
                 * make a row to filled col
                 */
                MySQLMessage mm = new MySQLMessage(pack.getRowData());
                mm.readUB3();
                mm.read();

                for (int i = 0; i < fieldCount; i++) {
                    byte[] colValue = mm.readBytesWithLength();
                    if (colValue != null)
                        unsafeRowWriter.write(i, colValue);
                    else
                        unsafeRow.setNullAt(i);
                }

                unsafeRow.setTotalSize(bufferHolder.totalSize());

                if (unsafeRowGrouper != null) {
                    unsafeRowGrouper.addRow(unsafeRow);
                } else if (globalSorter != null) {
                    globalSorter.insertRow(unsafeRow);
                } else {
                    globalMergeResult.insertRow(unsafeRow);
                }

                unsafeRow = null;
                bufferHolder = null;
                unsafeRowWriter = null;
            }

        } catch (final Exception e) {
            multiQueryHandler.handleDataProcessException(e);
        } finally {
            running.set(false);
            if (nulpack && !packs.isEmpty()) {
                this.run();
            }
        }
    }

    /**
     * release the resource of DataNodeMergeManager
     */
    public void clear() {

        if (unsafeRowGrouper != null) {
            unsafeRowGrouper.free();
            unsafeRowGrouper = null;
        }

        if (globalSorter != null) {
            globalSorter.cleanupResources();
            globalSorter = null;
        }

        if (globalMergeResult != null) {
            globalMergeResult.cleanupResources();
            globalMergeResult = null;
        }
    }
}
