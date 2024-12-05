/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.rwsplit.handle;

import com.oceanbase.obsharding_d.rwsplit.RWSplitNonBlockingSession;
import com.oceanbase.obsharding_d.services.rwsplit.CallbackFactory;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.util.Set;

/**
 * @author dcy
 * Create Date: 2020-12-15
 */
public final class TempTableHandler {
    private TempTableHandler() {
    }

    public static void handleCreate(String stmt, RWSplitService service, int offset) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        final SQLStatement sqlStatement = parser.parseCreate();
        if (!(sqlStatement instanceof SQLCreateTableStatement)) {
            throw new IllegalStateException("can't parse sql");
        }
        final RWSplitNonBlockingSession session = service.getSession2();
        final String sessionSchema = service.getSchema();
        session.execute(true, (isSuccess, resp, rwSplitService) -> {
            if (isSuccess) {
                final Set<String> tempTableSet = rwSplitService.getTmpTableSet();

                final SQLExprTableSource tableSource = ((SQLCreateTableStatement) sqlStatement).getTableSource();
                final String key = generateKey(tableSource, sessionSchema);
                tempTableSet.add(key);

            }

        });
    }

    private static String generateKey(SQLExprTableSource tableSource, String sessionSchema) {
        return generateKey(tableSource.getSchema(), tableSource.getName().getSimpleName(), sessionSchema);
    }

    private static String generateKey(String schemaName, String tableName, String sessionSchema) {
        if (schemaName == null) {
            return sessionSchema + "." + tableName;
        }
        return schemaName + "." + tableName;
    }

    public static void handleDrop(String stmt, RWSplitService service, int offset) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        final SQLStatement sqlStatement = parser.parseDrop();
        if (!(sqlStatement instanceof SQLDropTableStatement)) {
            throw new IllegalStateException("can't parse sql");
        }

        final RWSplitNonBlockingSession session = service.getSession2();
        final String sessionSchema = service.getSchema();
        session.execute(true, (isSuccess, resp, rwSplitService) -> {
            if (isSuccess) {
                final Set<String> tempTableSet = rwSplitService.getTmpTableSet();
                for (SQLExprTableSource tableSource : ((SQLDropTableStatement) sqlStatement).getTableSources()) {
                    final String key = generateKey(tableSource, sessionSchema);
                    tempTableSet.remove(key);
                }
            }
            CallbackFactory.TX_IMPLICITLYCOMMIT.callback(isSuccess, null, rwSplitService);
        });
    }
}
