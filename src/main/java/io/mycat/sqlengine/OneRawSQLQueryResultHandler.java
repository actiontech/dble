package io.mycat.sqlengine;

import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneRawSQLQueryResultHandler implements SQLJobHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(OneRawSQLQueryResultHandler.class);
    private Map<String, Integer> fetchColPosMap;
    private final SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callback;
    private final String[] fetchCols;
    private int fieldCount = 0;
    private Map<String, String> result = new HashMap<String, String>();

    public OneRawSQLQueryResultHandler(String[] fetchCols,
                                       SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callBack) {

        this.fetchCols = fetchCols;
        this.callback = callBack;
    }

    @Override
    public void onHeader(List<byte[]> fields) {
        fieldCount = fields.size();
        fetchColPosMap = new HashMap<String, Integer>();
        for (String watchFd : fetchCols) {
            for (int i = 0; i < fieldCount; i++) {
                byte[] field = fields.get(i);
                FieldPacket fieldPkg = new FieldPacket();
                fieldPkg.read(field);
                String fieldName = new String(fieldPkg.name);
                if (watchFd.equalsIgnoreCase(fieldName)) {
                    fetchColPosMap.put(fieldName, i);
                }
            }
        }

    }

    @Override
    public boolean onRowData(String dataNode, byte[] rowData) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
        rowDataPkg.read(rowData);
        String variableName = "";
        String variableValue = "";
        //fieldcount为2可能是select x也可能是show create table命令
        if (fieldCount == 2 && (fetchColPosMap.get("Variable_name") != null || fetchColPosMap.get("Value") != null)) {
            Integer ind = fetchColPosMap.get("Variable_name");
            if (ind != null) {
                byte[] columnData = rowDataPkg.fieldValues.get(ind);
                String columnVal = columnData != null ? new String(columnData) : null;
                variableName = columnVal;
            }
            ind = fetchColPosMap.get("Value");
            if (ind != null) {
                byte[] columnData = rowDataPkg.fieldValues.get(ind);
                String columnVal = columnData != null ? new String(columnData) : null;
                variableValue = columnVal;
            }
            result.put(variableName, variableValue);
        } else {
            for (String fetchCol : fetchCols) {
                Integer ind = fetchColPosMap.get(fetchCol);
                if (ind != null) {
                    byte[] columnData = rowDataPkg.fieldValues.get(ind);
                    String columnVal = columnData != null ? new String(columnData) : null;
                    result.put(fetchCol, columnVal);
                } else {
                    LOGGER.warn("cant't find column in sql query result " + fetchCol);
                }
            }
        }
        return false;
    }

    @Override
    public void finished(String dataNode, boolean failed) {
        SQLQueryResult<Map<String, String>> queryRestl = new SQLQueryResult<Map<String, String>>(this.result, !failed, dataNode);
        this.callback.onResult(queryRestl);

    }

    // 子类 MultiRowSQLQueryResultHandler 需要使用
    protected Map<String, String> getResult() {
        return result;
    }
}
