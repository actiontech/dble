package com.actiontech.dble.backend.datasource.check;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2019/12/23.
 */
public class CheckSumChecker extends AbstractConsistencyChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckSumChecker.class);

    public CheckSumChecker() {
    }

    @Override
    public String[] getFetchCols() {
        return new String[]{"Checksum"};
    }

    @Override
    public String getCountSQL(String dbName, String tName) {
        return "checksum table " + tName;
    }

    @Override
    public boolean resultEquals(SQLQueryResult<List<Map<String, String>>> or, SQLQueryResult<List<Map<String, String>>> cr) {
        Map<String, String> oresult = or.getResult().get(0);
        Map<String, String> cresult = cr.getResult().get(0);
        return (oresult.get("Checksum") == null && cresult.get("Checksum") == null) ||
                (oresult.get("Checksum") != null && cresult.get("Checksum") != null &&
                        oresult.get("Checksum").equals(cresult.get("Checksum")));
    }

    @Override
    public void failResponse(List<SQLQueryResult<List<Map<String, String>>>> res) {
        String tableId = schema + "." + tableName;
        String errorMsg = "Global Consistency Check fail for table :" + schema + "-" + tableName;
        LOGGER.warn(errorMsg);
        for (SQLQueryResult<List<Map<String, String>>> r : res) {
            LOGGER.info("Checksum is : " + r.getResult().get(0).get("Checksum"));
        }
        AlertUtil.alertSelf(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", tableId));
        ToResolveContainer.GLOBAL_TABLE_CONSISTENCY.add(tableId);
    }

    @Override
    public void resultResponse(List<SQLQueryResult<List<Map<String, String>>>> elist) {
        String tableId = schema + "." + tableName;

        if (elist.size() == 0) {
            LOGGER.info("Global Consistency Check success for table :" + schema + "-" + tableName);
            if (ToResolveContainer.GLOBAL_TABLE_CONSISTENCY.contains(tableId)) {
                AlertUtil.alertSelfResolve(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                        ToResolveContainer.GLOBAL_TABLE_CONSISTENCY, tableId);
            }
        } else {
            LOGGER.warn("Global Consistency Check fail for table :" + schema + "-" + tableName);
            StringBuilder sb = new StringBuilder("Error when check Global Consistency, Table ");
            sb.append(tableName).append(" shardingNode ");
            for (SQLQueryResult<List<Map<String, String>>> r : elist) {
                LOGGER.info("error node is : " + r.getTableName() + "-" + r.getShardingNode());
                sb.append(r.getShardingNode()).append(",");
            }
            sb.setLength(sb.length() - 1);
            AlertUtil.alertSelf(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, sb.toString(), AlertUtil.genSingleLabel("TABLE", tableId));
            ToResolveContainer.GLOBAL_TABLE_CONSISTENCY.add(tableId);
        }
    }
}
