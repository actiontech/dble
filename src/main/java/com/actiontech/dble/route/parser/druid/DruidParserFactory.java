/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.route.parser.druid.impl.*;
import com.actiontech.dble.route.parser.druid.impl.ddl.*;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;

import java.sql.SQLNonTransientException;

/**
 * DruidParserFactory
 *
 * @author wdw
 */
public final class DruidParserFactory {
    private DruidParserFactory() {
    }

    public static DruidParser create(SQLStatement statement, int sqlType)
            throws SQLNonTransientException {
        DruidParser parser = null;
        if (statement instanceof SQLSelectStatement) {
            parser = new DruidSelectParser();
        } else if (statement instanceof MySqlInsertStatement) {
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement) {
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlReplaceStatement) {
            parser = new DruidReplaceParser();
        } else if (statement instanceof MySqlUpdateStatement) {
            parser = new DruidUpdateParser();
        } else if (statement instanceof MySqlLockTableStatement) {
            parser = new DruidLockTableParser();
        } else if (statement instanceof SQLDDLStatement) {
            if (statement instanceof MySqlCreateTableStatement) {
                parser = new DruidCreateTableParser();
            } else if (statement instanceof SQLDropTableStatement) {
                parser = new DruidDropTableParser();
            } else if (statement instanceof SQLAlterTableStatement) {
                parser = new DruidAlterTableParser();
            } else if (statement instanceof SQLCreateIndexStatement) {
                parser = new DruidCreateIndexParser();
            } else if (statement instanceof SQLDropIndexStatement) {
                parser = new DruidDropIndexParser();
            } else {
                String msg = "THE DDL is not supported :" + statement;
                throw new SQLNonTransientException(msg);
            }
        } else if (statement instanceof SQLTruncateStatement) {
            parser = new DruidTruncateTableParser();
        } else if (sqlType == ServerParse.DDL) {
            String msg = "THE DDL is not supported :" + statement;
            throw new SQLNonTransientException(msg);
        } else {
            parser = new DefaultDruidParser();
        }

        return parser;
    }
}
