/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.parser;

import com.oceanbase.obsharding_d.server.parser.PrepareChangeVisitor;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.alibaba.druid.util.JdbcConstants.MYSQL;

/**
 * @author dcy
 * Create Date: 2021-01-21
 */
public class PrepareChangeVisitorTest {

    @Test
    public void check1() {
        check("select * from test inner join t1 on t1.id=t2.id or t1.id=?  where id=? and id2 in(?,?) and id3=1 and ( id4=1 and id5=1 ) and id2 in(1,?)  and id9  in (select * from test) and  exists (select * from test where id between ? and ?);");
    }

    @Test
    public void check2() {
        check("/*注释内容*/ SELECT id2 from t1 order by 'id2'   ");
    }

    @Test
    public void check3() {
        check("select fun(?) as A,t.*,*,id where id in(?,?) group by ?  having ? ");
    }

    @Test
    public void check4() {
        final String str = check("select ?");
        String target = "/* used for prepare statement. */\n" +
                "SELECT true\n" +
                "LIMIT 0";
        Assert.assertEquals(target, str);
    }

    private String check(String sql3) {
        final List<SQLStatement> statements = SQLUtils.parseStatements(sql3, MYSQL, true);
        for (SQLStatement statement : statements) {
            final PrepareChangeVisitor visitor = new PrepareChangeVisitor();
            statement.accept(visitor);
            System.out.println(statement.toString());
            Assert.assertTrue("doesn't clear all placeholder", !statement.toString().contains("?"));
            return statement.toString();
        }
        return null;
    }

}
