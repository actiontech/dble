/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.util.StringUtil;

/**
 * @author collapsar
 */
public final class DbleHintParser {

    public static final int OTHER = -1;
    public static final int SQL = 1;
    public static final int SHARDING_NODE = 2;
    public static final int DB_TYPE = 3;
    public static final int DB_INSTANCE_URL = 4;
    public static final int PLAN = 5;

    public static final int UPROXY_DEST = 11;
    public static final int UPROXY_MASTER = 12;

    private DbleHintParser() {
    }

    public static HintInfo parse(String sql) {
        int j = 0;
        int len = sql.length();
        int hintStartPosition = 0;
        if (sql.charAt(j++) == '/' && sql.charAt(j++) == '*') {
            for (; j < len; j++) {
                switch (sql.charAt(j)) {
                    case ' ':
                        continue;
                    case '#':
                    case '!':
                        char[] annotation = Versions.ANNOTATION_NAME.toCharArray();
                        for (char dble : annotation) {
                            if (dble != sql.charAt(++j)) {
                                return null;
                            }
                        }
                        hintStartPosition = dbleHint(sql, j);
                        if (hintStartPosition == -1) {
                            return null;
                        }
                        continue;
                    case '*':
                        if (len > ++j && sql.charAt(j) == '/') {
                            int start = hintStartPosition >> 8;
                            int endPos = j;
                            return new HintInfo(sql.substring(start + 1, --endPos), sql.substring(++j), hintStartPosition & 0xff);
                        }
                        continue;
                    default:
                        break;
                }
            }
        }
        return null;
    }

    public static HintInfo parseRW(String sql) {
        HintInfo hintInfo = parse(sql);
        if (hintInfo != null) {
            return hintInfo;
        }
        hintInfo = parseUproxyHint(sql);
        return hintInfo;
    }

    private static HintInfo parseUproxyHint(String sql) {
        if (StringUtil.isEmpty(sql) || !sql.contains("/*")) {
            return null;
        }
        String[] leftSplit = sql.split("/\\*");
        if (leftSplit.length != 2 || !leftSplit[1].contains("*/")) {
            return null;
        }
        String[] rightSplit = leftSplit[1].split("\\*/");
        String content = rightSplit[0].trim();
        if (StringUtil.equalsIgnoreCase(content, "master")) {
            return new HintInfo("master", sql, UPROXY_MASTER);
        } else if (content.length() > 12 && content.substring(0, 12).equalsIgnoreCase("uproxy_dest:")) {
            return new HintInfo(content.substring(12), sql, UPROXY_DEST);
        }
        return null;
    }


    private static int dbleHint(String sql, int offset) {
        if (sql.length() > ++offset) {
            switch (sql.charAt(offset)) {
                case 'S':
                case 's':
                    return sCheck(sql, offset);
                case 'D':
                case 'd':
                    return dCheck(sql, offset);
                case 'P':
                case 'p':
                    return planCheck(sql, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int sCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'H':
                case 'h':
                    return shardingNodeCheck(stmt, offset);
                case 'q':
                case 'Q':
                    return sqlCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int dCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'B' || c1 == 'b') && c2 == '_') {
                switch (c3) {
                    case 'T':
                    case 't':
                        return dbTypeCheck(stmt, offset);
                    case 'I':
                    case 'i':
                        return dbInstanceUrlCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    private static int shardingNodeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
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
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'D' || c3 == 'd') && (c4 == 'I' || c4 == 'i') &&
                    (c5 == 'N' || c5 == 'n') && (c6 == 'G' || c6 == 'g') &&
                    (c7 == 'N' || c7 == 'n') && (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'D' || c9 == 'd') && (c10 == 'E' || c10 == 'e')) {
                offset = ParseUtil.skipSpaceUtil(stmt, ++offset, '=');
                if (offset == -1) {
                    return OTHER;
                } else {
                    return (offset << 8) | SHARDING_NODE;
                }
            }
        }
        return OTHER;
    }

    private static int sqlCheck(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            if (c1 == 'L' || c1 == 'l') {
                offset = ParseUtil.skipSpaceUtil(stmt, ++offset, '=');
                if (offset == -1) {
                    return OTHER;
                } else {
                    return (offset << 8) | SQL;
                }
            }
        }
        return OTHER;
    }

    private static int dbTypeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'Y' || c1 == 'y') && (c2 == 'P' || c2 == 'p') &&
                    (c3 == 'E' || c3 == 'e')) {
                offset = ParseUtil.skipSpaceUtil(stmt, ++offset, '=');
                if (offset == -1) {
                    return OTHER;
                } else {
                    return (offset << 8) | DB_TYPE;
                }
            }
        }
        return OTHER;
    }

    private static int dbInstanceUrlCheck(String stmt, int offset) {
        if (stmt.length() > offset + 11) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            if ((c2 == 'N' || c2 == 'n') && (c3 == 'S' || c3 == 's') &&
                    (c4 == 'T' || c4 == 't') && (c5 == 'A' || c5 == 'a') &&
                    (c6 == 'N' || c6 == 'n') && (c7 == 'C' || c7 == 'c') &&
                    (c8 == 'E' || c8 == 'e') && (c9 == '_') &&
                    (c10 == 'U' || c10 == 'u') && (c11 == 'R' || c11 == 'r') &&
                    (c12 == 'L' || c12 == 'l')) {
                offset = ParseUtil.skipSpaceUtil(stmt, ++offset, '=');
                if (offset == -1) {
                    return OTHER;
                } else {
                    return (offset << 8) | DB_INSTANCE_URL;
                }
            }
        }
        return OTHER;
    }

    private static int planCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'l' || c1 == 'L') && (c2 == 'A' || c2 == 'a') &&
                    (c3 == 'N' || c3 == 'n')) {
                offset = ParseUtil.skipSpaceUtil(stmt, ++offset, '=');
                if (offset == -1) {
                    return OTHER;
                } else {
                    return (offset << 8) | PLAN;
                }
            }
        }
        return OTHER;
    }

    public static class HintInfo {
        private final String hintValue;
        private final String realSql;
        private final int type;

        HintInfo(String hintValue, String realSql, int type) {
            hintValue = hintValue.trim();
            if (hintValue.endsWith("'") && hintValue.startsWith("'")) {
                hintValue = hintValue.substring(1, hintValue.length() - 1);
            }
            this.hintValue = hintValue;
            this.realSql = realSql.trim();
            this.type = type;
        }

        public String getHintValue() {
            return hintValue;
        }

        public String getRealSql() {
            return realSql;
        }

        public int getType() {
            return type;
        }
    }

}
