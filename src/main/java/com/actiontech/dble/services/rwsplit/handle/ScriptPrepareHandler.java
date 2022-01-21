/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.parser.util.DruidUtil;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;

import java.sql.SQLSyntaxErrorException;

/**
 * @author dcy
 * Create Date: 2020-12-15
 */
public final class ScriptPrepareHandler {
    private ScriptPrepareHandler() {
    }

    public static void handle(RWSplitService service, String stmt) {
        try {
            SQLStatement statement = DruidUtil.parseMultiSQL(stmt);
            if (statement instanceof MySqlPrepareStatement) {
                String simpleName = ((MySqlPrepareStatement) statement).getName().getSimpleName();
                service.getNameSet().add(simpleName);
            }
            if (statement instanceof MysqlDeallocatePrepareStatement) {
                String simpleName = ((MysqlDeallocatePrepareStatement) statement).getStatementName().getSimpleName();
                service.getNameSet().remove(simpleName);
            }
        } catch (SQLSyntaxErrorException e) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");
        }
        service.getSession().execute(true, null);
    }
}
