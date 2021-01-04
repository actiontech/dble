/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.store.CursorCache;
import com.actiontech.dble.backend.mysql.store.CursorCacheForGeneral;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatement.class);
    private long id;
    private String statement;
    private int columnsNumber;
    private int parametersNumber;
    private int[] parametersType;
    private CursorCache cursorCache;
    private List<FieldPacket> fieldPackets;
    private Consumer<Integer> prepareCallback = null;

    /**
     * store the byte data from COM_STMT_SEND_LONG_DATA
     * <pre>
     * key : param_id
     * value : byte data
     * </pre>
     */
    private Map<Long, ByteArrayOutputStream> longDataMap;

    public PreparedStatement(long id, String statement, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.parametersNumber = parametersNumber;
        this.parametersType = new int[parametersNumber];
        this.longDataMap = new HashMap<>();
    }

    public long getId() {
        return id;
    }

    public String getStatement() {
        return statement;
    }

    public int getColumnsNumber() {
        return columnsNumber;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    public int[] getParametersType() {
        return parametersType;
    }

    public ByteArrayOutputStream getLongData(long paramId) {
        return longDataMap.get(paramId);
    }

    public void setColumnsNumber(int columnsNumber) {
        this.columnsNumber = columnsNumber;
    }

    public void initCursor(NonBlockingSession session, ResponseHandler responseHandler, int fieldCount, List<FieldPacket> tmpFieldPackets) {
        if (cursorCache != null) {
            cursorCache.close();
            LOGGER.warn("cursor in one prepareStatement init twice. Maybe something wrong");
        }
        if (session.getShardingService().getRequestScope().isUsingJoin()) {
            /*
            todo:could use optimized implementation here
             */
            cursorCache = new CursorCacheForGeneral(fieldCount);
        } else {
            cursorCache = new CursorCacheForGeneral(fieldCount);
        }

        this.fieldPackets = tmpFieldPackets;
    }

    public List<FieldPacket> getFieldPackets() {
        return fieldPackets;
    }

    public CursorCache getCursorCache() {
        return cursorCache;
    }


    public void setPrepareCallback(Consumer<Integer> prepareCallback) {
        this.prepareCallback = prepareCallback;
    }


    public void onPrepareOk(int columnCount) {
        prepareCallback.accept(columnCount);
    }

    /**
     * reset value which is used by COM_STMT_RESET
     */
    public void resetLongData() {
        for (Map.Entry<Long, ByteArrayOutputStream> longData : longDataMap.entrySet()) {
            longData.getValue().reset();
        }
    }

    /**
     * append data to param
     *
     * @param paramId
     * @param data
     * @throws IOException
     */
    public void appendLongData(long paramId, byte[] data) throws IOException {
        if (getLongData(paramId) == null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(data);
            longDataMap.put(paramId, out);
        } else {
            longDataMap.get(paramId).write(data);
        }
    }


    public Map<Long, ByteArrayOutputStream> getLongDataMap() {
        return longDataMap;
    }

    @Override
    public void close() {
        if (cursorCache != null) {
            cursorCache.close();
        }
    }
}
