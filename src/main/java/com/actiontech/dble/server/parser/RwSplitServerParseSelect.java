/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

import java.util.regex.Pattern;

/**
 * @author mycat
 */
public final class RwSplitServerParseSelect {
    private RwSplitServerParseSelect() {
    }

    public static final int OTHER = -1;
    public static final int SELECT_FOR_UPDATE = 2;
    public static final int LOCK_IN_SHARE_MODE = 3;
    public static final int SELECT_VAR_ALL = 9;

    private static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile("^.*(\\s+for\\s+update)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOCK_IN_SHARE_MODE_PATTERN = Pattern.compile("^.*(\\s+lock\\s+in\\s+share\\s+mode)\\s*$", Pattern.CASE_INSENSITIVE);


    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case '@':
                    return select2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }


    public static int parseSpecial(String stmt) {

        if (SELECT_FOR_UPDATE_PATTERN.matcher(stmt).matches()) {
            return SELECT_FOR_UPDATE;
        }
        if (LOCK_IN_SHARE_MODE_PATTERN.matcher(stmt).matches()) {
            return LOCK_IN_SHARE_MODE;
        }
        return OTHER;
    }


    /**
     * SELECT @@session.auto_increment_increment
     *
     * @param stmt
     * @param offset
     * @return
     */
    private static int sessionVarCheck(String stmt, int offset) {
        String s = stmt.substring(offset).toLowerCase();
        if (!s.startsWith("session.")) {
            return OTHER;
        }
        s = s.substring(8);
        if (s.startsWith("auto_increment_increment")) {
            if (s.contains("@@")) {
                return SELECT_VAR_ALL;
            }

        }
        return OTHER;
    }


    static int select2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {

                case 's':
                case 'S':
                    return sessionVarCheck(stmt, offset);

                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

}
