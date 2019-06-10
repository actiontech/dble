/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

import com.actiontech.dble.config.Versions;

/**
 * @author mycat
 */
public final class ParseUtil {
    private ParseUtil() {
    }

    private static boolean isEOF(char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';');
    }

    public static boolean isEOF(String stmt, int offset) {
        boolean isInhint = false;
        for (; offset < stmt.length(); offset++) {
            if (!isInhint) {
                if (!ParseUtil.isEOF(stmt.charAt(offset))) {
                    if (stmt.charAt(offset) == '/' &&
                            ((offset + 1) < stmt.length() && stmt.charAt(++offset) == '*')) {
                        isInhint = true;
                    } else {
                        return false;
                    }
                }
            } else {
                if (stmt.charAt(offset) == '*' &&
                        ((offset + 1) < stmt.length() && stmt.charAt(++offset) == '/')) {
                    isInhint = false;
                }
            }
        }

        return !isInhint;
    }

    public static boolean isMultiEof(String stmt, int offset) {
        for (; offset < stmt.length(); offset++) {
            char c = stmt.charAt(offset);
            if (c == ';') {
                return true;
            } else if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    public static boolean isSpace(char space) {
        return space == ' ' || space == '\r' || space == '\n' || space == '\t';
    }


    public static boolean isSpaceOrLeft(char space) {
        return space == ' ' || space == '\r' || space == '\n' || space == '\t' || space == '(';
    }

    /**
     * check the tail is unexpected char
     *
     * @param offset
     * @param stmt
     * @return
     */
    public static boolean isErrorTail(int offset, String stmt) {
        for (; offset < stmt.length(); offset++) {
            if (!ParseUtil.isSpace(stmt.charAt(offset))) {
                return true;
            }
        }
        return false;
    }

    public static String parseString(String stmt) {
        int offset = stmt.indexOf('=');
        if (offset != -1) {
            int length = stmt.length();
            while (length > ++offset && stmt.charAt(offset) == ' ') {
                // do nothing
            }
            return stmt.substring(offset).trim();
        }
        return null;
    }

    /**
     * <code>'abc'</code>
     *
     * @param offset stmt.charAt(offset) == first <code>'</code>
     */
    private static String parseString(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                    case '0':
                        sb.append('\0');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'Z':
                        sb.append((char) 26);
                        break;
                    default:
                        sb.append(c);
                }
            } else if (c == '\'') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '\'') {
                    ++offset;
                    sb.append('\'');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static int getSQLId(String stmt, int start) {
        int offset = start;
        while (stmt.length() > offset) {
            if (!isSpace(stmt.charAt(offset))) {
                break;
            }
            offset++;
        }
        if (stmt.charAt(offset) == '=' && stmt.length() > ++offset) {
            String id = stmt.substring(offset).trim();
            try {
                return Integer.parseInt(id);
            } catch (NumberFormatException e) {
                //ignore error
            }
        }
        return -1;
    }

    /**
     * <code>"abc"</code>
     *
     * @param offset stmt.charAt(offset) == first <code>"</code>
     */
    private static String parseString2(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                    case '0':
                        sb.append('\0');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'Z':
                        sb.append((char) 26);
                        break;
                    default:
                        sb.append(c);
                }
            } else if (c == '"') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '"') {
                    ++offset;
                    sb.append('"');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>AS `abc`</code>
     *
     * @param offset stmt.charAt(offset) == first <code>`</code>
     */
    private static String parseIdentifierEscape(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '`') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '`') {
                    ++offset;
                    sb.append('`');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * @param aliasIndex for <code>AS id</code>, index of 'i'
     */
    public static String parseAlias(String stmt, final int aliasIndex) {
        if (aliasIndex < 0 || aliasIndex >= stmt.length()) {
            return null;
        }
        switch (stmt.charAt(aliasIndex)) {
            case '\'':
                return parseString(stmt, aliasIndex);
            case '"':
                return parseString2(stmt, aliasIndex);
            case '`':
                return parseIdentifierEscape(stmt, aliasIndex);
            default:
                int offset = aliasIndex;
                for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset) {
                    //do nothing
                }
                return stmt.substring(aliasIndex, offset);
        }
    }

    /**
     * return the index of the end of hint in stmt
     *
     * @param stmt
     * @param offset
     * @return
     */
    public static int comment(String stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        switch (stmt.charAt(n)) {
            case '/':
                if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1) {
                    for (int i = n; i < len; ++i) {
                        if (stmt.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && stmt.charAt(m) == '/') {
                                return m;
                            }
                        }
                    }
                }
                break;
            case '#':
                for (int i = n + 1; i < len; ++i) {
                    if (stmt.charAt(i) == '\n') {
                        return i;
                    }
                }
                break;
            default:
                break;
        }
        return offset;
    }


    /**
     * return the index of the end of hint in stmt
     * return -1 if the comment is a dble hint
     *
     * @param stmt
     * @param offset
     * @return
     */
    public static int commentHint(String stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        char[] annotation = Versions.ANNOTATION_NAME.toCharArray();
        switch (stmt.charAt(n)) {
            case '/':
                if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1) {
                    if (stmt.length() > n + 6) {
                        int x = n;
                        char ch1 = stmt.charAt(x);
                        if ((ch1 == '!' || ch1 == '#')) {
                            boolean flag = true;
                            for (char y : annotation) {
                                if (stmt.charAt(++x) != y) {
                                    flag = false;
                                    break;
                                }
                            }
                            if (flag) {
                                return -1;
                            }
                        }
                    }
                    for (int i = n; i < len; ++i) {
                        if (stmt.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && stmt.charAt(m) == '/') {
                                return m;
                            }
                        }
                    }
                }
                break;
            default:
                break;
        }
        return offset;
    }


    public static boolean currentCharIsSep(String stmt, int offset) {
        if (stmt.length() > offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    return true;
                default:
                    return false;
            }
        }
        return true;
    }

    public static int move(String stmt, int offset, int length) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                case '#':
                    i = comment(stmt, i);
                    continue;
                default:
                    return i + length;
            }
        }
        return i;
    }

    public static boolean compare(String s, int offset, char[] keyword) {
        if (s.length() >= offset + keyword.length) {
            for (int i = 0; i < keyword.length; ++i, ++offset) {
                if (Character.toUpperCase(s.charAt(offset)) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    public static int findNextBreak(String sql) {
        return findNextBreak(sql, false);
    }

    /**
     * @param sql
     * @param inProcedure
     * @return
     */
    private static int findNextBreak(String sql, boolean inProcedure) {
        //is the char in apostrophe
        boolean inApostrophe = false;
        //is the char after procedure begin
        boolean procedureBegin = false;
        String bodyLeft = null;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '\\':
                    i++;
                    break;
                case '\'':
                case '\"':
                    if (!inApostrophe) {
                        inApostrophe = true;
                        bodyLeft = String.valueOf(c);
                    } else {
                        String rightTest = String.valueOf(c);
                        if (rightTest.equals(bodyLeft)) {
                            inApostrophe = false;
                        }
                    }
                    break;
                case ';':
                    if (!inApostrophe && !procedureBegin) {
                        return i;
                    }
                    break;
                case 'p':
                case 'P':
                    if (!inApostrophe) {
                        i = getProcedureEndPos(sql, i);
                    }
                    break;
                case 'B':
                    if (inProcedure && !inApostrophe && isBegin(sql, i)) {
                        procedureBegin = true;
                        i = i + 4;
                    }
                    break;
                case 'E':
                    if (!inApostrophe && procedureBegin && isEnd(sql, i)) {
                        procedureBegin = false;
                        i = i + 2;
                    }
                    break;
                default:
            }
        }
        return sql.length() - 1;
    }

    private static boolean isBegin(String stmt, int offset) {
        if (offset <= 0 || !ParseUtil.isSpace(stmt.charAt(offset - 1)) || stmt.length() <= offset + "EGIN ".length()) {
            return false;
        }
        char c1 = stmt.charAt(++offset);
        char c2 = stmt.charAt(++offset);
        char c3 = stmt.charAt(++offset);
        char c4 = stmt.charAt(++offset);
        char c5 = stmt.charAt(++offset);
        return c1 == 'E' &&
                c2 == 'G' &&
                c3 == 'I' &&
                c4 == 'N' &&
                ParseUtil.isSpace(c5);
    }

    private static boolean isEnd(String stmt, int offset) {
        if (offset <= 0 || !ParseUtil.isSpace(stmt.charAt(offset - 1)) || stmt.length() <= offset + "ND".length()) {
            return false;
        }
        char c1 = stmt.charAt(++offset);
        char c2 = stmt.charAt(++offset);
        if (c1 == 'N' && c2 == 'D') {
            offset++;
            for (; offset < stmt.length(); offset++) {
                char tmp = stmt.charAt(offset);
                if (ParseUtil.isSpace(tmp)) {
                    continue;
                }
                return tmp == ';';
            }
            return true;
        }
        return false;
    }

    private static int getProcedureEndPos(String stmt, int offset) {
        if (stmt.length() > offset + "ROCEDURE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'r' || c1 == 'R') &&
                    (c2 == 'o' || c2 == 'O') &&
                    (c3 == 'c' || c3 == 'C') &&
                    (c4 == 'e' || c4 == 'E') &&
                    (c5 == 'd' || c5 == 'D') &&
                    (c6 == 'u' || c6 == 'U') &&
                    (c7 == 'r' || c7 == 'R') &&
                    (c8 == 'e' || c8 == 'E')) {
                String testSql = stmt.substring(++offset).toUpperCase();
                //offset + length  -1 for checking ';' outside
                return offset - 1 + findNextBreak(testSql, true);
            }
        }
        return offset;
    }
}
