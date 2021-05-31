/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.util.SplitUtil;

/**
 * @author mycat
 */
public final class ManagerParseStart {
    private ManagerParseStart() {
    }

    public static final int OTHER = -1;
    public static final int STATISTIC_QUEUE_MONITOR = 1;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case '@':
                    return startCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    public static Pair<String[], Integer> getPair(String stmt) {
        int offset = stmt.indexOf("@@");
        String s = stmt.substring(offset + 11).trim();
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

    // STATISTIC_QUEUE_TIMER
    static int startCheck(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > offset + 21 && stmt.substring(offset + 1).toUpperCase().startsWith("STATISTIC_QUEUE_MONITOR")) {
                return STATISTIC_QUEUE_MONITOR;
            }
        }
        return OTHER;
    }

}
