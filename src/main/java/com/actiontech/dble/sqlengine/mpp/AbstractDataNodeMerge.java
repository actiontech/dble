/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeQueryHandler;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zagnix on 2016/7/6.
 */
public abstract class AbstractDataNodeMerge implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(AbstractDataNodeMerge.class);
    /**
     * col size
     */
    protected int fieldCount;

    /**
     * cache the router
     */
    protected final RouteResultset rrs;
    protected MultiNodeQueryHandler multiQueryHandler = null;
    /**
     * the end packet
     */
    protected PackWraper endFlagPack = new PackWraper();


    /**
     * rowData queue
     */
    protected BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<>();

    /**
     * the merge thread is running
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public AbstractDataNodeMerge(MultiNodeQueryHandler handler, RouteResultset rrs) {
        this.rrs = rrs;
        this.multiQueryHandler = handler;
    }


    /**
     * Add a row pack, and may be wake up a business thread to work if not running.
     *
     * @param pack row pack
     * @return true wake up a business thread, otherwise false
     * @author Uncle-pan
     * @since 2016-03-23
     */
    protected final boolean addPack(final PackWraper pack) {
        packs.add(pack);
        if (running.get()) {
            return false;
        }
        final DbleServer server = DbleServer.getInstance();
        server.getBusinessExecutor().execute(this);
        return true;
    }

    /**
     * PackWraper the new row data
     * process new record (mysql binary data),if data can output to client
     * ,return true
     *
     * @param dataNode DN's name (data from this dataNode)
     * @param rowData  raw data
     */
    public boolean onNewRecord(String dataNode, byte[] rowData) {
        final PackWraper data = new PackWraper();
        data.setDataNode(dataNode);
        data.setRowData(rowData);
        addPack(data);

        return false;
    }


    /**
     * get the index array of row according map
     *
     * @param columns
     * @param toIndexMap
     * @return
     */
    protected static int[] toColumnIndex(String[] columns, Map<String, ColMeta> toIndexMap) {
        int[] result = new int[columns.length];
        ColMeta curColMeta;
        for (int i = 0; i < columns.length; i++) {
            curColMeta = toIndexMap.get(columns[i].toUpperCase());
            if (curColMeta == null) {
                throw new IllegalArgumentException(
                        "all columns in group by clause should be in the selected column list.!" + columns[i]);
            }
            result[i] = curColMeta.getColIndex();
        }
        return result;
    }

    @Override
    public abstract void run();

    public abstract void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldSize) throws IOException;

    public void outputMergeResult(NonBlockingSession session, byte[] eof) {
        addPack(endFlagPack);
    }

    public RouteResultset getRrs() {
        return this.rrs;
    }

    /**
     * @return (i*(offset+size) row)
     */
    public abstract List<RowDataPacket> getResults(byte[] eof);

    public abstract void clear();

}
