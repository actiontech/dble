/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser.druid;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.impl.DruidUpdateParser;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/7
 */

public class DruidUpdateParserTest {
    /**
     * testUpdateShardColumn
     *
     * @throws NoSuchMethodException
     */
    @Test
    public void testUpdateShardColumn() throws NoSuchMethodException {
        throwExceptionParse("update hotnews set id = 1 where name = 234;", true);
        throwExceptionParse("update hotnews set id = 1 where id = 3;", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name = '234'", false);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 or name = '234'", true);
        throwExceptionParse("update hotnews set id = 'A', name = '123' where id = 'A' and name = '234'", false);
        throwExceptionParse("update hotnews set id = 'A', name = '123' where id = 'A' or name = '234'", true);
        throwExceptionParse("update hotnews set id = 1.5, name = '123' where id = 1.5 and name = '234'", false);
        throwExceptionParse("update hotnews set id = 1.5, name = '123' where id = 1.5 or name = '234'", true);

        throwExceptionParse("update hotnews set id = 1, name = '123' where name = '234' and (id = 1 or age > 3)", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and (name = '234' or age > 3)", false);

        // subQuery /between
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name in (select name from test)", false);
        throwExceptionParse("update hotnews set id = 1, name = '123' where name = '123' and id in (select id from test)", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3 and name = '234'", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3 or name = '234'", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name between '124' and '234'", false);
    }

    /**
     * testAliasUpdateShardColumn
     *
     * @throws NoSuchMethodException
     */
    @Test
    public void testAliasUpdateShardColumn() throws NoSuchMethodException {
        throwExceptionParse("update hotnews h set h.id = 1 where h.name = 234;", true);
        throwExceptionParse("update hotnews h set h.id = 1 where h.id = 3;", true);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 or h.name = '234'", true);
        throwExceptionParse("update hotnews h set h.id = 'A', h.name = '123' where h.id = 'A' and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 'A', h.name = '123' where h.id = 'A' or h.name = '234'", true);
        throwExceptionParse("update hotnews h set h.id = 1.5, h.name = '123' where h.id = 1.5 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1.5, h.name = '123' where h.id = 1.5 or h.name = '234'", true);

        throwExceptionParse("update hotnews h set id = 1, h.name = '123' where h.id = 1 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where id = 1 or h.name = '234'", true);

        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.name = '234' and (h.id = 1 or h.age > 3)", true);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 and (h.name = '234' or h.age > 3)", false);
    }

    public void throwExceptionParse(String sql, boolean throwException) throws NoSuchMethodException {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);
        MySqlUpdateStatement update = (MySqlUpdateStatement) sqlStatement;
        SchemaConfig schemaConfig = mock(SchemaConfig.class);
        Map<String, TableConfig> tables = mock(Map.class);
        TableConfig tableConfig = mock(TableConfig.class);
        String tableName = "hotnews";
        when((schemaConfig).getTables()).thenReturn(tables);
        when(tables.get(tableName)).thenReturn(tableConfig);
        when(tableConfig.getParentTC()).thenReturn(null);
        RouteResultset routeResultset = new RouteResultset(sql, 11);
        Class c = DruidUpdateParser.class;
        Method method = c.getDeclaredMethod("confirmShardColumnNotUpdated", new Class[]{SQLUpdateStatement.class, SchemaConfig.class, String.class, String.class, String.class, RouteResultset.class});
        method.setAccessible(true);
        try {
            method.invoke(c.newInstance(), update, schemaConfig, tableName, "ID", "", routeResultset);
            if (throwException) {
                System.out.println("Not passed without exception is not correct");
                Assert.assertTrue(false);
            } else {
                System.out.println("Passed without exception. Maybe the partition key exists in update statement,but not update in fact");
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            if (throwException) {
                System.out.println(e.getCause().getClass());
                Assert.assertTrue(e.getCause() instanceof SQLNonTransientException);
                System.out.println("SQLNonTransientException is expected");
            } else {
                System.out.println("need checked");
                Assert.assertTrue(false);
            }
        }
    }

    /*
    * add an static to printf where ,eg:
    * update mytab t set t.ptn_col = 'A', col1 = 3 where ptn_col = 'A' and (col1 = 4 or col2 > 5);
    * where looks like this
    *                  AND
    *              /        \
    *             =          OR
    *          /   \       /    \
    *     ptn_col 'A'    =       >
    *                  /  \    /   \
    *               col1  4  col2   5
    * the output is
    * BooleanAnd			Num of nodes in next level: 2
    * Equality	BooleanOr			Num of nodes in next level: 4
    * ptn_col	'A'	Equality	Equality			Num of nodes in next level: 4
    * col1	4	col2	5			Num of nodes in next level: 0
    *
     */
    public static void printWhereClauseAST(SQLExpr sqlExpr) {
        if (sqlExpr == null)
            return;
        ArrayList<SQLExpr> exprNode = new ArrayList<>();
        int i = 0, curLevel = 1, nextLevel = 0;
        SQLExpr iterExpr;
        exprNode.add(sqlExpr);
        while (true) {
            iterExpr = exprNode.get(i++);
            if (iterExpr == null)
                break;

            if (iterExpr instanceof SQLBinaryOpExpr) {
                System.out.print(((SQLBinaryOpExpr) iterExpr).getOperator());
            } else {
                System.out.print(iterExpr.toString());
            }
            System.out.print("\t");
            curLevel--;

            if (iterExpr instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr) iterExpr).getLeft() != null) {
                    exprNode.add(((SQLBinaryOpExpr) iterExpr).getLeft());
                    nextLevel++;
                }
                if (((SQLBinaryOpExpr) iterExpr).getRight() != null) {
                    exprNode.add(((SQLBinaryOpExpr) iterExpr).getRight());
                    nextLevel++;
                }
            }
            if (curLevel == 0) {
                System.out.println("\t\tNum of nodes in next level: " + nextLevel);
                curLevel = nextLevel;
                nextLevel = 0;
            }
            if (exprNode.size() == i)
                break;
        }
    }
}
