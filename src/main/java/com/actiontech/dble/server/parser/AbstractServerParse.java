/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

import java.util.regex.Pattern;

/**
 * @author dcy
 */
public abstract class AbstractServerParse implements ServerParse {
    protected AbstractServerParse() {
    }


    private static final Pattern PATTERN = Pattern.compile("(load)+\\s+(data)+\\s+\\w*\\s*(infile)+", Pattern.CASE_INSENSITIVE);
    protected static final Pattern CALL_PATTERN = Pattern.compile("\\w*\\;\\s*\\s*(call)+\\s+\\w*\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile(".*(\\s+for\\s+update)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOCK_IN_SHARE_MODE_PATTERN = Pattern.compile(".*(\\s+lock\\s+in\\s+share\\s+mode)\\s*$", Pattern.CASE_INSENSITIVE);


    @Override
    public int parseSpecial(int sqlType, String stmt) {
        if (ServerParse.SELECT != sqlType) {
            return OTHER;
        }
        if (SELECT_FOR_UPDATE_PATTERN.matcher(stmt).matches()) {
            return SELECT_FOR_UPDATE;
        }
        if (LOCK_IN_SHARE_MODE_PATTERN.matcher(stmt).matches()) {
            return LOCK_IN_SHARE_MODE;
        }
        return OTHER;
    }

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
}
