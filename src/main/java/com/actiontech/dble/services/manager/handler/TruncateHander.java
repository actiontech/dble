package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;

public final class TruncateHander {

    private TruncateHander() {
    }

    public static void handle(String sql, ManagerService service, int offset) {
        String tableName = sql.substring(offset).trim();
        String schema = null;
        int index = tableName.indexOf(".");
        if (index > 0) {
            schema = tableName.substring(0, index).trim();
            tableName = tableName.substring(1 + index).trim();
        }
        if (schema == null && (schema = service.getSchema()) == null) {
            throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "3D000", "No database selected");
        } else {
            if (!schema.equals(ManagerSchemaInfo.SCHEMA_NAME)) {
                throw new RuntimeException("schema " + schema + " doesn't exist!");
            }
            ManagerBaseTable managerTable = ManagerSchemaInfo.getInstance().getTables().get(tableName);
            if (managerTable == null) {
                throw new RuntimeException("table " + tableName + " doesn't exist!");
            } else {
                if (managerTable.isTruncate()) {
                    managerTable.truncate();
                    OkPacket ok = new OkPacket();
                    ok.setPacketId(1);
                    ok.write(service.getConnection());
                } else {
                    service.writeErrMessage("42000", "Access denied for table '" + managerTable.getTableName() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
                }
            }
        }
    }
}
