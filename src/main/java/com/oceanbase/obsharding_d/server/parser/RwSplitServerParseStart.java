/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class RwSplitServerParseStart extends ServerParseStart {
    public RwSplitServerParseStart() {
    }

    @Override
    protected int transactionCheck(String stmt, int offset) {
        if (stmt.length() > offset + 10) {
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
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'S' || c4 == 's') && (c5 == 'A' || c5 == 'a') && (c6 == 'C' || c6 == 'c') &&
                    (c7 == 'T' || c7 == 't') && (c8 == 'I' || c8 == 'i') && (c9 == 'O' || c9 == 'o') &&
                    (c10 == 'N' || c10 == 'n')) {
                if (stmt.length() == ++offset)
                    return ServerParse.START_TRANSACTION;
                int currentOffset = ParseUtil.skipSpace(stmt, offset);
                if (stmt.length() == currentOffset) {
                    return ServerParse.START_TRANSACTION;
                } else {
                    return ServerParse.OTHER;
                }
            }
        }
        return ServerParse.OTHER;
    }
}
