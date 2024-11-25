/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.backend.mysql.store.CursorCache;
import com.oceanbase.obsharding_d.backend.mysql.store.CursorCacheForGeneral;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.parser.PrepareStatementParseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatement.class);
    private final long id;
    private final String statement;
    private volatile int columnsNumber;
    private final int parametersNumber;
    private final int[] parametersType;
    private volatile CursorCache cursorCache;
    private volatile List<FieldPacket> fieldPackets;
    private volatile Consumer<Integer> prepareCallback = null;

    /**
     * store the byte data from COM_STMT_SEND_LONG_DATA
     * <pre>
     * key : param_id
     * value : byte data
     * </pre>
     */
    private Map<Long, ByteArrayOutputStream> longDataMap;
    private PrepareStatementParseInfo parseInfo;

    public PreparedStatement(long id, String statement, PrepareStatementParseInfo parseInfo) {
        this.id = id;
        this.statement = statement;
        this.parametersNumber = parseInfo.getArgumentSize();
        this.parametersType = new int[parseInfo.getArgumentSize()];
        this.longDataMap = new HashMap<>();
        this.parseInfo = parseInfo;
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
            cursorCache = new CursorCacheForGeneral(fieldCount, session.getSource().generateBufferRecordBuilder());
        } else {
            cursorCache = new CursorCacheForGeneral(fieldCount, session.getSource().generateBufferRecordBuilder());
        }

        this.fieldPackets = new ArrayList<>(tmpFieldPackets);
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


    public String toComQuery(BindValue[] bindValues) {
        return parseInfo.toComQuery(bindValues, this.parametersType);
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
