/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author collapsar
 * Create Date: 2021-12-06
 */
public final class DruidUtil {
    private DruidUtil() {
    }

    public static SQLStatement parseMultiSQL(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);

        /*
         * thrown SQL SyntaxError if parser error
         */
        List<SQLStatement> list;
        try {
            list = parser.parseStatementList();
        } catch (Exception t) {
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException(t.getMessage());
            } else {
                throw new SQLSyntaxErrorException(t);
            }
        }
        if (list.size() > 1) {
            throw new SQLSyntaxErrorException("MultiQueries is not supported,use single query instead ");
        }
        return list.get(0);
    }

    public static SQLStatement parseSQL(String originSql) throws SQLSyntaxErrorException {
        SQLStatementParser parser = new MySqlStatementParser(originSql);
        try {
            return parser.parseStatement(true);
        } catch (Exception t) {
            if (t.getMessage() != null) {
                throw new SQLSyntaxErrorException("druid not support sql syntax, the reason is " + t.getMessage());
            } else {
                throw new SQLSyntaxErrorException("druid not support sql syntax, the reason is " + t);
            }
        }
    }
}
