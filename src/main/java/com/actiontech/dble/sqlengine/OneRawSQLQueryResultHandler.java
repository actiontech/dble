/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneRawSQLQueryResultHandler implements SQLJobHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(OneRawSQLQueryResultHandler.class);
    private final SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callback;
    private final String[] fetchCols;
    private Map<String, Integer> fetchColPosMap;
    private int fieldCount = 0;
    private Map<String, String> result = new HashMap<>();

    public OneRawSQLQueryResultHandler(String[] fetchCols,
                                       SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callBack) {

        this.fetchCols = fetchCols;
        this.callback = callBack;
    }

    @Override
    public void onHeader(List<byte[]> fields) {
        result.clear();
        fieldCount = fields.size();
        fetchColPosMap = new HashMap<>();
        for (String watchFd : fetchCols) {
            for (int i = 0; i < fieldCount; i++) {
                byte[] field = fields.get(i);
                FieldPacket fieldPkg = new FieldPacket();
                fieldPkg.read(field);
                String fieldName = new String(fieldPkg.getName());
                if (watchFd.equalsIgnoreCase(fieldName)) {
                    fetchColPosMap.put(fieldName, i);
                }
            }
        }

    }

    @Override
    public void onRowData(byte[] rowData) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
        rowDataPkg.read(rowData);
        String variableName = "";
        String variableValue = "";
        //if fieldcount is 2,it may be select x or show create table
        if (fieldCount == 2 && (fetchColPosMap.get("Variable_name") != null || fetchColPosMap.get("Value") != null)) {
            Integer ind = fetchColPosMap.get("Variable_name");
            if (ind != null) {
                byte[] columnData = rowDataPkg.fieldValues.get(ind);
                variableName = columnData != null ? new String(columnData) : null;
            }
            ind = fetchColPosMap.get("Value");
            if (ind != null) {
                byte[] columnData = rowDataPkg.fieldValues.get(ind);
                variableValue = columnData != null ? new String(columnData) : null;
            }
            result.put(variableName, variableValue);
        } else {
            for (Map.Entry<String, Integer> entry : fetchColPosMap.entrySet()) {
                Integer ind = entry.getValue();
                if (ind != null) {
                    byte[] columnData = rowDataPkg.fieldValues.get(ind);
                    String columnVal = columnData != null ? new String(columnData) : null;
                    result.put(entry.getKey(), columnVal);
                }
            }
        }
    }

    @Override
    public void finished(String shardingNode, boolean failed) {
        SQLQueryResult<Map<String, String>> queryResult = new SQLQueryResult<>(this.result, !failed, shardingNode);
        this.callback.onResult(queryResult);
    }

    //  MultiRowSQLQueryResultHandler need
    protected Map<String, String> getResult() {
        return new HashMap<>(result);
    }

    public SQLQueryResultListener<SQLQueryResult<Map<String, String>>> getCallback() {
        return callback;
    }
}
