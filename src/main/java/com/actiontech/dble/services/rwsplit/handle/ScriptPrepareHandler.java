package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLSyntaxErrorException;

public final class ScriptPrepareHandler {

    private ScriptPrepareHandler() {
    }

    public static void handle(RWSplitService service, String stmt) {
        try {
            SQLStatement statement = parseSQL(stmt);
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

    private static SQLStatement parseSQL(String stmt) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        try {
            return parser.parseStatement();
        } catch (Exception t) {
            throw new SQLSyntaxErrorException(t);
        }
    }

}
