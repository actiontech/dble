/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

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
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') && (c3 == 'B' || c3 == 'b') &&
                    (c4 == 'E' || c4 == 'e') && (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't')) {
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
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'E' || c2 == 'e') && (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') && (c5 == 'I' || c5 == 'i') && (c6 == 'L' || c6 == 'l')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return ManagerParseShow.HEARTBEAT_DETAIL;
            }
        }
        return OTHER;
    }
}
