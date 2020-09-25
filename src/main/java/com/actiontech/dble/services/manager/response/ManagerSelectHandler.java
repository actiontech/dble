/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.sqlengine.TransformSQLJob;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public final class ManagerSelectHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerSelectHandler.class);

    public ManagerSelectHandler() {
    }

    public void execute(ManagerService service, String stmt) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        SQLStatement statement;
        try {
            statement = parser.parseStatement();
        } catch (ParserException e) {
            LOGGER.warn("Unsupported select:" + stmt);
            service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement " + stmt);
            return;
        }
        if (!(statement instanceof SQLSelectStatement)) {
            service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            return;
        }
        SQLSelectStatement selectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery sqlSelectQuery = ((SQLSelectStatement) statement).getSelect().getQuery();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
            SQLTableSource mysqlFrom = selectQueryBlock.getFrom();
            if (mysqlFrom == null) {
                noTableSelect(service, stmt, selectQueryBlock.getSelectList());
            } else {
                service.getSession2().execute(service.getSchema(), selectStatement);
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            service.getSession2().execute(service.getSchema(), selectStatement);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

    private void noTableSelect(ManagerService service, String stmt, List<SQLSelectItem> items) {
        for (SQLSelectItem item : items) {
            SQLExpr selectItem = item.getExpr();
            if (!isVariantRef(selectItem)) {
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                return;
            }
        }
        Iterator<PhysicalDbGroup> iterator = DbleServer.getInstance().getConfig().getDbGroups().values().iterator();
        if (iterator.hasNext()) {
            PhysicalDbGroup pool = iterator.next();
            final PhysicalDbInstance source = pool.getWriteDbInstance();
            TransformSQLJob sqlJob = new TransformSQLJob(stmt, null, source, service);
            sqlJob.run();
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "no valid dbGroup/dbInstance");
        }
    }

    private boolean isVariantRef(SQLExpr expr) {
        if (expr instanceof SQLVariantRefExpr) {
            return true;
        } else if (expr instanceof SQLPropertyExpr) {
            return isVariantRef(((SQLPropertyExpr) expr).getOwner());
        } else {
            return false;
        }
    }

}
