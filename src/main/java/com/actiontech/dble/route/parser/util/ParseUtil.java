/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

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
        for (; offset < stmt.length(); offset++) {
            if (!ParseUtil.isEOF(stmt.charAt(offset))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSpace(char space) {
        return space == ' ' || space == '\r' || space == '\n' || space == '\t';
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
    //FIXME SIZE CHECK
    public static String parseString(String stmt) {
        int offset = stmt.indexOf('=');
        while (stmt.charAt(++offset) == ' ') {
            //do nothing
        }
        if (offset != -1 && stmt.length() > offset) {
            String txt = stmt.substring(offset).trim();
            return txt;
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

    public static long getSQLId(String stmt) {
        int offset = stmt.indexOf('=');
        if (offset != -1 && stmt.length() > ++offset) {
            String id = stmt.substring(offset).trim();
            try {
                return Long.parseLong(id);
            } catch (NumberFormatException e) {
                //ignore error
            }
        }
        return 0L;
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

}
