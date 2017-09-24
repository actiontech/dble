/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

/**
 * @author mycat
 */
public final class ManagerParseRollback {
    private ManagerParseRollback() {
    }

    public static final int OTHER = -1;
    public static final int CONFIG = 1;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '@':
                    return rollback2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int rollback2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' &&
                stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return rollback2CCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // ROLLBACK @@CONFIG
    private static int rollback2CCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'F' || c3 == 'f') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'G' || c5 == 'g')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return CONFIG;
            }
        }
        return OTHER;
    }


}
