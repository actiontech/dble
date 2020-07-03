/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.LinkedList;
import java.util.List;

public final class ScriptPrepareParse {
    public static final int OTHER = -1;
    public static final int PREPARE = 1;
    public static final int EXECUTE = 2;
    public static final int DROP = 3;

    private ScriptPrepareParse() {
    }

    public static int parse(String stmt, int offset, ShardingService service) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    break;
                case 'P':
                case 'p':
                    return prepareParse(stmt, i, service);
                case 'E':
                case 'e':
                    return executeParse(stmt, i, service);
                case 'D':
                case 'd':
                    return dParse(stmt, i, service);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static boolean needSpaceAndComment(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c = stmt.charAt(offset + 1);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '/' || c == '#') {
                return true;
            }
        }
        return false;
    }

    private static int skipSpaceAndComment(String stmt, int offset) {
        int i = ++offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    break;
                default:
                    return i - 1;
            }
        }
        return i - 1;
    }

    private static int prepareParse(String stmt, int offset, ShardingService service) {
        String name = null;

        offset += "REPARE".length();
        if (needSpaceAndComment(stmt, offset)) {
            offset = skipSpaceAndComment(stmt, offset);
            for (int i = ++offset; i < stmt.length(); i++) {
                char c1 = stmt.charAt(i);
                if (c1 == ' ' || c1 == '\t' || c1 == '\r' || c1 == '\n' || c1 == '/' || c1 == '#') {
                    name = stmt.substring(offset, i).toUpperCase();
                    offset = i - 1;
                    break;
                }
            }
            if (offset + 1 == stmt.length()) {
                return OTHER;
            }

            if (needSpaceAndComment(stmt, offset)) {
                offset = skipSpaceAndComment(stmt, offset);
                if (stmt.length() > offset + "FROM".length()) {
                    char c1 = stmt.charAt(++offset);
                    char c2 = stmt.charAt(++offset);
                    char c3 = stmt.charAt(++offset);
                    char c4 = stmt.charAt(++offset);
                    if ((c1 == 'F' || c1 == 'f') && (c2 == 'R' || c2 == 'r') && (c3 == 'O' || c3 == 'o') && (c4 == 'M' || c4 == 'm')) {
                        if (needSpaceAndComment(stmt, offset)) {
                            offset = skipSpaceAndComment(stmt, offset);
                            char c5 = stmt.charAt(++offset);
                            switch (c5) {
                                case '\'':
                                case '"':
                                    return parseStmtFrom(stmt, offset, service, name);
                                case '@':
                                    return parseStmtFromUser(stmt, offset, service, name);
                                default:
                                    break;
                            }
                        }
                    }
                }
            }
        }
        return OTHER;
    }

    private static int parseStmtFrom(String stmt, int offset, ShardingService service, String name) {
        String exestmt = null;

        char c1 = stmt.charAt(offset);
        int i = stmt.lastIndexOf(c1);
        exestmt = stmt.substring(++offset, i);
        exestmt = StringEscapeUtils.unescapeJava(exestmt);
        offset = skipSpaceAndComment(stmt, i);
        if (offset + 1 != stmt.length()) {
            return OTHER;
        }
        service.getSptPrepare().setName(name);
        service.getSptPrepare().setExePrepare(exestmt, false);
        return PREPARE;
    }

    private static int parseStmtFromUser(String stmt, int offset, ShardingService service, String name) {
        String exestmt = null;
        int i = ++offset;
        for (; i < stmt.length(); i++) {
            char c1 = stmt.charAt(i);
            if (c1 == ' ' || c1 == '\t' || c1 == '\r' || c1 == '\n' || c1 == '/' || c1 == '#') {
                exestmt = stmt.substring(offset, i).toUpperCase();
                offset = skipSpaceAndComment(stmt, i);
                if (offset + 1 != stmt.length()) {
                    return OTHER;
                }
                service.getSptPrepare().setName(name);
                service.getSptPrepare().setExePrepare(exestmt, true);
                return PREPARE;
            }
        }
        exestmt = stmt.substring(offset, i).toUpperCase();
        service.getSptPrepare().setName(name);
        service.getSptPrepare().setExePrepare(exestmt, true);
        return PREPARE;
    }

    public static void parseStmt(String stmt, List<String> parts) {
        int start = 0;
        for (int i = 0; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    break;
                case '?':
                    parts.add(stmt.substring(start, i));
                    start = i + 1;
                    break;
                default:
                    break;
            }
        }
        parts.add(stmt.substring(start, stmt.length()));
    }

    private static int executeParse(String stmt, int offset, ShardingService service) {
        String name = null;

        offset += "XECUTE".length();
        if (needSpaceAndComment(stmt, offset)) {
            offset = skipSpaceAndComment(stmt, offset);
            int i = ++offset;
            for (; i < stmt.length(); i++) {
                char c1 = stmt.charAt(i);
                if (c1 == ' ' || c1 == '\t' || c1 == '\r' || c1 == '\n' || c1 == '/' || c1 == '#') {
                    name = stmt.substring(offset, i).toUpperCase();
                    break;
                }
            }
            if (i == stmt.length()) {
                name = stmt.substring(offset, i).toUpperCase();
                service.getSptPrepare().setName(name);
                service.getSptPrepare().setArguments(null);
                return EXECUTE;
            } else {
                offset = skipSpaceAndComment(stmt, i);
                if (stmt.length() > offset + "USING".length()) {
                    char c1 = stmt.charAt(++offset);
                    char c2 = stmt.charAt(++offset);
                    char c3 = stmt.charAt(++offset);
                    char c4 = stmt.charAt(++offset);
                    char c5 = stmt.charAt(++offset);
                    if ((c1 == 'U' || c1 == 'u') && (c2 == 'S' || c2 == 's') && (c3 == 'I' || c3 == 'i') && (c4 == 'N' || c4 == 'n') &&
                        (c5 == 'G' || c5 == 'g')) {
                        if (needSpaceAndComment(stmt, offset)) {
                            offset = skipSpaceAndComment(stmt, offset);
                            List<String> arguments = new LinkedList<String>();
                            argsParse(stmt, offset, arguments);
                            service.getSptPrepare().setName(name);
                            service.getSptPrepare().setArguments(arguments);
                            return EXECUTE;
                        }
                    }
                }
            }
        }
        return OTHER;
    }

    private static void argsParse(String stmt, int offset, List<String> arguments) {
        int i = offset;
        while (i < stmt.length()) {
            char c1 = stmt.charAt(++i);
            if (c1 == '@') {
                i = argName(stmt, i, arguments);
            }
        }
    }

    private static int argName(String stmt, int offset, List<String> arguments) {
        int i = ++offset;
        for (; i < stmt.length(); i++) {
            char c1 = stmt.charAt(i);
            switch (c1) {
                case ',':
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                case '/':
                case '#':
                    arguments.add(stmt.substring(offset, i).toUpperCase());
                    i = skipSpaceAndComment(stmt, i);
                    return i;
                default:
                    break;
            }
        }
        arguments.add(stmt.substring(offset, i).toUpperCase());
        return i;
    }

    private static int dParse(String stmt, int offset, ShardingService service) {
        if (stmt.length() > offset) {
            char c1 = stmt.charAt(++offset);
            switch (c1) {
                case 'E':
                case 'e':
                    return dropParse(stmt, offset, service, false);
                case 'R':
                case 'r':
                    return dropParse(stmt, offset, service, true);
                default:
                    break;
            }
        }
        return OTHER;
    }

    private static int dropParse(String stmt, int offset, ShardingService service, boolean isdrop) {
        String name;
        if (isdrop) {
            offset += "OP".length();
        } else {
            offset += "ALLOCATE".length();
        }

        if (needSpaceAndComment(stmt, offset)) {
            offset = skipSpaceAndComment(stmt, offset);
            if (stmt.length() > offset + "PREPARE".length()) {
                char c1 = stmt.charAt(++offset);
                char c2 = stmt.charAt(++offset);
                char c3 = stmt.charAt(++offset);
                char c4 = stmt.charAt(++offset);
                char c5 = stmt.charAt(++offset);
                char c6 = stmt.charAt(++offset);
                char c7 = stmt.charAt(++offset);
                if ((c1 == 'P' || c1 == 'p') && (c2 == 'R' || c2 == 'r') && (c3 == 'E' || c3 == 'e') && (c4 == 'P' || c4 == 'p') &&
                    (c5 == 'A' || c5 == 'a') && (c6 == 'R' || c6 == 'r') && (c7 == 'E' || c7 == 'e')) {
                    if (needSpaceAndComment(stmt, offset)) {
                        offset = skipSpaceAndComment(stmt, offset);
                        int i = ++offset;
                        for (; i < stmt.length(); i++) {
                            char c8 = stmt.charAt(i);
                            if (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n' || c8 == '/' || c8 == '#') {
                                name = stmt.substring(offset, i).toUpperCase();
                                offset = skipSpaceAndComment(stmt, i);
                                if (offset + 1 == stmt.length()) {
                                    service.getSptPrepare().setName(name);
                                }
                                return DROP;
                            }
                        }
                        name = stmt.substring(offset, i).toUpperCase();
                        service.getSptPrepare().setName(name);
                        return DROP;
                    }
                }
            }
        }
        return OTHER;
    }
}
