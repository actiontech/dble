/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.SplitUtil;

/**
 * @author mycat
 */
public final class ManagerParseSwitch {
    private ManagerParseSwitch() {
    }

    public static final int OTHER = -1;
    public static final int DATASOURCE = 1;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '@':
                    return switch2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    public static Pair<String[], Integer> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(offset + 12).trim();
        int p1 = s.lastIndexOf(':');
        if (p1 == -1) {
            String[] src = SplitUtil.split(s, ',', '$', '-', '[', ']');
            return new Pair<>(src, null);
        } else {
            String[] src = SplitUtil.split(s, ':', true);
            String[] src1 = SplitUtil.split(src[0], ',', '$', '-', '[', ']');
            return new Pair<>(src1, Integer.valueOf(src[1]));
        }
    }

    // DATASOURCE
    static int switch2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > offset + 10) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') && (c5 == 'S' || c5 == 's') && (c6 == 'O' || c6 == 'o') &&
                    (c7 == 'U' || c7 == 'u') && (c8 == 'R' || c8 == 'r') && (c9 == 'C' || c9 == 'c') &&
                    (c10 == 'E' || c10 == 'e')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return DATASOURCE;
            }
        }
        return OTHER;
    }

}
