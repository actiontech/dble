/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * @author dcy
 */
public abstract class AbstractServerParse implements ServerParse {

    static final Pattern CALL_PATTERN = Pattern.compile("\\w*\\;\\s*\\s*(call)+\\s+\\w*\\s*", Pattern.CASE_INSENSITIVE);

    @Override
    public boolean startWithHint(String stmt) {
        int length = stmt.length();
        for (int i = 0; i < length; ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                    if (i == 0 && stmt.charAt(1) == '*' && stmt.charAt(2) == '!' && stmt.charAt(length - 2) == '*' &&
                            stmt.charAt(length - 1) == '/') {
                        return false;
                    }
                    //fall through
                case '#':
                    i = ParseUtil.commentHint(stmt, i);
                    if (i == -1) {
                        return true;
                    }
                    continue;
                default:
                    break;
            }
            break;
        }
        return false;
    }


    @Override
    public boolean isMultiStatement(String sql) {
        int index = ParseUtil.findNextBreak(sql);
        return index + 1 < sql.length() && !ParseUtil.isEOF(sql, index);
    }

    @Override
    public void getMultiStatement(String sql, LinkedList<String> splitSql) {
        int index = ParseUtil.findNextBreak(sql);
        if (index + 1 < sql.length() && !ParseUtil.isEOF(sql, index)) {
            splitSql.add(sql.substring(0, index).trim());
            sql = sql.substring(index + 1);
            getMultiStatement(sql, splitSql);
        } else {
            if (sql.endsWith(";")) {
                splitSql.add(sql.substring(0, sql.length() - 1).trim());
            } else {
                splitSql.add(sql.trim());
            }
        }
    }

    public static boolean isTCL(int sqlType) {
        switch (sqlType) {
            case RwSplitServerParse.BEGIN:
            case RwSplitServerParse.START_TRANSACTION:
            case RwSplitServerParse.COMMIT:
            case RwSplitServerParse.ROLLBACK:
                return true;
            default:
                return false;
        }
    }

    public static boolean isImplicitlyCommitSql(int sqlType) {
        switch (sqlType) {
            case ServerParse.DDL:
            case ServerParse.ALTER_VIEW:
            case ServerParse.CREATE_DATABASE:
            case ServerParse.CREATE_VIEW:
            case ServerParse.DROP_VIEW:
            case ServerParse.DROP_TABLE:
            case RwSplitServerParse.INSTALL:
            case RwSplitServerParse.RENAME:
            case RwSplitServerParse.UNINSTALL:
            case RwSplitServerParse.GRANT:
            case RwSplitServerParse.REVOKE:
            case ServerParse.LOCK:
                return true;
            default:
                return false;
        }
    }
}
