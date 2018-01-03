/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.sql.visitor;

import com.alibaba.druid.DruidRuntimeException;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.util.Utils;

import java.math.BigInteger;
import java.util.List;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE;

public class ActionSQLEvalVisitorUtils extends SQLEvalVisitorUtils {
    public static Object eval(String dbType, SQLObject sqlObject, List<Object> parameters, boolean throwError) {
        ActionMySqlEvalVisitorImpl visitor = new ActionMySqlEvalVisitorImpl();
        visitor.setParameters(parameters);
        sqlObject.accept(visitor);
        Object value = getValue(sqlObject);
        if (value == null && throwError && !sqlObject.getAttributes().containsKey("eval.value")) {
            throw new DruidRuntimeException("eval error : " + SQLUtils.toSQLString(sqlObject, dbType));
        } else {
            return value;
        }
    }
    public static boolean visit(ActionMySqlEvalVisitorImpl visitor, SQLBinaryExpr x) {
        String text = x.getValue();

        long[] words = new long[text.length() / 64 + 1];
        for (int i = text.length() - 1; i >= 0; --i) {
            char ch = text.charAt(i);
            if (ch == '1') {
                int wordIndex = i >> 6;
                words[wordIndex] |= (1L << (text.length() - 1 - i));
            }
        }

        Object val;

        if (words.length == 1) {
            val = words[0];
        } else {
            byte[] bytes = new byte[words.length * 8];

            for (int i = 0; i < words.length; ++i) {
                Utils.putLong(bytes, (words.length - 1 - i) * 8, words[i]);
            }

            val = new BigInteger(bytes);
        }

        x.putAttribute(EVAL_VALUE, val);

        return false;
    }
}
