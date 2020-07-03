/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement {

    private long id;
    private String statement;
    private int columnsNumber;
    private int parametersNumber;
    private int[] parametersType;
    /**
     * store the byte data from COM_STMT_SEND_LONG_DATA
     * <pre>
     * key : param_id
     * value : byte data
     * </pre>
     */
    private Map<Long, ByteArrayOutputStream> longDataMap;

    public PreparedStatement(long id, String statement, int columnsNumber, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.columnsNumber = columnsNumber;
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

}
