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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Created by zagnix on 2016/6/21.
 */
public class DataNodeMergeManager extends AbstractDataNodeMerge {

    private static final Logger LOGGER = Logger.getLogger(DataNodeMergeManager.class);

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


    public void onRowMetaData(Map<String, ColMeta> columnToIndex, int fieldSize) throws IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("field metadata keys:" + columnToIndex.keySet());
            LOGGER.debug("field metadata values:" + columnToIndex.values());
        }
        this.fieldCount = fieldSize;
        String[] groupByCols = rrs.getGroupByCols();
        unsafeRowGrouper = new UnsafeRowGrouper(columnToIndex, groupByCols);
        // 1.schema
        StructType schema = new StructType(columnToIndex, fieldSize);
        if (groupByCols != null) {
            OrderCol[] orderCols = new OrderCol[groupByCols.length];
            for (int i = 0; i < groupByCols.length; i++) {
                orderCols[i] = new OrderCol(columnToIndex.get(groupByCols[i].toUpperCase()));
            }
            schema.setOrderCols(orderCols);
        }
        //2 .PrefixComputer
        UnsafeExternalRowSorter.PrefixComputer prefixComputer = new RowPrefixComputer(schema);

        //3 .PrefixComparator ,ASC/DESC and the default is ASC

        PrefixComparator prefixComparator = PrefixComparators.LONG;


        DataNodeMemoryManager dataNodeMemoryManager = new DataNodeMemoryManager(memoryManager,
                Thread.currentThread().getId());

        globalMergeResult = new UnsafeExternalRowSorter(
                dataNodeMemoryManager,
                serverMemory,
                schema,
                prefixComparator,
                prefixComputer,
                conf.getSizeAsBytes("server.buffer.pageSize", "1m"),
                false,
                true);
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

        boolean nulPack = false;

        try {
            for (; ; ) {
                final PackWraper pack = packs.poll();

                if (pack == null) {
                    nulPack = true;
                    break;
                }
                if (pack == endFlagPack) {
                    /*
                     * if last date node send row eof packet
                     * means all the data have received
                     */
                    final int warningCount = 0;
                    final EOFPacket eofPacket = new EOFPacket();
                    final ByteBuffer eof = ByteBuffer.allocate(9);
                    BufferUtil.writeUB3(eof, eofPacket.calcPacketSize());
                    eof.put(eofPacket.getPacketId());
                    eof.put(eofPacket.getFieldCount());
                    BufferUtil.writeUB2(eof, warningCount);
                    BufferUtil.writeUB2(eof, eofPacket.getStatus());
                    final ServerConnection source = multiQueryHandler.getSession().getSource();
                    final byte[] array = eof.array();


                    Iterator<UnsafeRow> iterator;
                    if (unsafeRowGrouper != null) {
                        iterator = unsafeRowGrouper.getResult(globalMergeResult);
                    } else {
                        iterator = globalMergeResult.sort();
                    }

                    if (iterator != null) {
                        multiQueryHandler.outputMergeResult(source, array, iterator);
                    }

                    if (unsafeRowGrouper != null) {
                        unsafeRowGrouper.free();
                        unsafeRowGrouper = null;
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

                // make a row to filled col
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
                } else {
                    globalMergeResult.insertRow(unsafeRow);
                }
            }

        } catch (final Exception e) {
            multiQueryHandler.handleDataProcessException(e);
        } finally {
            running.set(false);
            if (nulPack && !packs.isEmpty()) {
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

        if (globalMergeResult != null) {
            globalMergeResult.cleanupResources();
            globalMergeResult = null;
        }
    }
}
