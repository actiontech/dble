/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

public final class TruncateHander {

    private TruncateHander() {
    }

    public static void handle(String sql, ManagerService service) {
        SQLStatement sqlStatement = new MySqlStatementParser(sql).parseStatement();
        if (sqlStatement instanceof SQLTruncateStatement) {
            String schema = null;
            String tableName = null;
            SQLExpr tableExpr = ((SQLTruncateStatement) sqlStatement).getTableSources().get(0).getExpr();
            if (tableExpr instanceof SQLPropertyExpr) {
                tableName = StringUtil.removeApostropheOrBackQuote(((SQLPropertyExpr) tableExpr).getName());
                schema = StringUtil.removeApostropheOrBackQuote(((SQLPropertyExpr) tableExpr).getOwner().toString());
            } else if (tableExpr instanceof SQLIdentifierExpr) {
                tableName = StringUtil.removeApostropheOrBackQuote(((SQLIdentifierExpr) tableExpr).getName());
            }
            if (schema == null && (schema = service.getSchema()) == null) {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "3D000", "No database selected");
            } else {
                if (!schema.equals(ManagerSchemaInfo.SCHEMA_NAME)) {
                    throw new RuntimeException("schema `" + schema + "` doesn't exist!");
                }
                ManagerBaseTable managerTable = ManagerSchemaInfo.getInstance().getTables().get(tableName);
                if (managerTable == null) {
                    throw new RuntimeException("table `" + tableName + "` doesn't exist!");
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
}
