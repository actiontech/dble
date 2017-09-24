/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.util.StringUtil;

/**
 * @author songwie
 */
public final class ManagerParseHeartbeat {
    private ManagerParseHeartbeat() {
    }

    public static final int OTHER = -1;

    // SHOW @@HEARTBEAT
    static int show2HeaCheck(String stmt, int offset) {
        if (stmt.length() > offset + "RTBEAT".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') & (c3 == 'B' || c3 == 'b') &&
                    (c4 == 'E' || c4 == 'e') & (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't')) {
                if (stmt.length() > offset + ".DETAIL".length()) {
                    char c7 = stmt.charAt(++offset);
                    if (c7 == '.') {
                        return show2HeaDetailCheck(stmt, offset);
                    }
                }
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return ManagerParseShow.HEARTBEAT;
            }
        }
        return OTHER;
    }

    // SHOW @@HEARTBEAT.DETAIL
    static int show2HeaDetailCheck(String stmt, int offset) {
        if (stmt.length() > offset + "DETAIL".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'E' || c2 == 'e') & (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') & (c5 == 'I' || c5 == 'i') && (c6 == 'L' || c6 == 'l')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return ManagerParseShow.HEARTBEAT_DETAIL;
            }
        }
        return OTHER;
    }

    public static Pair<String, String> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(++offset + " heartbeat.detail".length());
        if (StringUtil.isEmpty(s)) {
            return new Pair<>("name", "");
        }
        char c = s.charAt(0);
        offset = 0;
        if (c == ' ' && s.length() > " where name=".length()) {
            offset = ManagerParseShow.trim(++offset, s);
            char c1 = s.charAt(offset);
            char c2 = s.charAt(++offset);
            char c3 = s.charAt(++offset);
            char c4 = s.charAt(++offset);
            char c5 = s.charAt(++offset);
            char c6 = s.charAt(++offset);
            //char c11 = s.charAt(++offset);
            if ((c1 == 'W' || c1 == 'w') && (c2 == 'H' || c2 == 'h') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'R' || c4 == 'r') && (c5 == 'E' || c5 == 'e') && (c6 == ' ')) {
                offset = ManagerParseShow.trim(++offset, s);
                char c7 = s.charAt(offset);
                char c8 = s.charAt(++offset);
                char c9 = s.charAt(++offset);
                char c10 = s.charAt(++offset);
                if ((c7 == 'N' || c7 == 'n') && (c8 == 'A' || c8 == 'a') && (c9 == 'M' || c9 == 'm') &&
                        (c10 == 'E' || c10 == 'e')) {
                    offset = ManagerParseShow.trim(++offset, s);
                    if (s.charAt(offset) == '=') {
                        offset = ManagerParseShow.trim(++offset, s);
                        String name = s.substring(offset).trim();
                        if (!name.contains(" ") && !name.contains("\r") && !name.contains("\n") &&
                                !name.contains("\t")) {
                            return new Pair<>("name", name);
                        }
                    }
                }

            }
        }
        return new Pair<>("name", "");
    }

}
