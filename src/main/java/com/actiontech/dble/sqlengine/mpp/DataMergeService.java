/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.backend.mysql.BufferUtil;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeQueryHandler;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/

/**
 * Data merge service handle data Min,Max,AVG group . order by . limit
 *
 * @author wuzhih /modify by coder_czp/2015/11/2
 * <p>
 * Fixbug: sql timeout and hang problem.
 * @author Uncle-pan
 * @since 2016-03-23
 */
public class DataMergeService extends AbstractDataNodeMerge {

    private RowDataPacketGrouper grouper;
    private Map<String, LinkedList<RowDataPacket>> result = new HashMap<>();
    private static final Logger LOGGER = Logger.getLogger(DataMergeService.class);

    public DataMergeService(MultiNodeQueryHandler handler, RouteResultset rrs) {
        super(handler, rrs);

        for (RouteResultsetNode node : rrs.getNodes()) {
            result.put(node.getName(), new LinkedList<RowDataPacket>());
        }
    }


    /**
     * @param columnToIndex columnToIndex
     * @param fieldSize fieldSize
     */
    public void onRowMetaData(Map<String, ColMeta> columnToIndex, int fieldSize) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("field metadata keys:" + columnToIndex.keySet());
            LOGGER.debug("field metadata values:" + columnToIndex.values());
        }

        int[] groupColumnIndexes = null;
        this.fieldCount = fieldSize;

        if (rrs.getGroupByCols() != null) {

            groupColumnIndexes = toColumnIndex(rrs.getGroupByCols(), columnToIndex);
        }
        grouper = new RowDataPacketGrouper(groupColumnIndexes);
    }


    /**
     * release resources
     */
    public void clear() {
        result.clear();
        grouper = null;
    }

    @Override
    public void run() {
        // sort-or-group: no need for us to using multi-threads, because
        //both sorter and group are synchronized!!
        // @author Uncle-pan
        // @since 2016-03-23
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // eof handler has been placed to "if (pack == END_FLAG_PACK){}" in for-statement
        // @author Uncle-pan
        // @since 2016-03-23
        boolean nulPack = false;
        try {
            // loop-on-packs
            for (; ; ) {
                final PackWraper pack = packs.poll();
                // async: handling row pack queue, this business thread should exit when no pack
                // @author Uncle-pan
                // @since 2016-03-23
                if (pack == null) {
                    nulPack = true;
                    break;
                }
                // eof: handling eof pack and exit
                if (pack == endFlagPack) {


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
                    multiQueryHandler.outputMergeResult(source, array, getResults(array));
                    break;
                }


                // merge: sort-or-group, or simple add
                final RowDataPacket row = new RowDataPacket(fieldCount);
                row.read(pack.getRowData());

                if (grouper != null) {
                    grouper.addRow(row);
                } else {
                    result.get(pack.getDataNode()).add(row);
                }
            } // rof
        } catch (final Exception e) {
            multiQueryHandler.handleDataProcessException(e);
        } finally {
            running.set(false);
        }
        // try to check packs, it's possible that adding a pack after polling a null pack
        //and before this time pointer!!
        // @author Uncle-pan
        // @since 2016-03-23
        if (nulPack && !packs.isEmpty()) {
            this.run();
        }
    }


    /**
     * return merged data  (i * (offset + size) rows at most)
     *
     * @return list
     */
    public List<RowDataPacket> getResults(byte[] eof) {

        List<RowDataPacket> tmpResult = null;

        if (this.grouper != null) {
            tmpResult = grouper.getResult();
            grouper = null;
        }

        //no grouper and sorter
        if (tmpResult == null) {
            tmpResult = new LinkedList<>();
            for (RouteResultsetNode node : rrs.getNodes()) {
                tmpResult.addAll(result.get(node.getName()));
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("prepare mpp merge result for " + rrs.getStatement());
        }
        return tmpResult;
    }
}

