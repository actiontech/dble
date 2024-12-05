/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author dcy
 */
public abstract class AbstractServerParse implements ServerParse {

    static final Pattern CALL_PATTERN = Pattern.compile("\\w*\\;\\s*\\s*(call)+\\s+\\w*\\s*", Pattern.CASE_INSENSITIVE);
    static final Pattern AUTOCOMMIT_PATTERN = Pattern.compile("^\\s*set\\s+(([a-zA-Z0-9]+\\s*=\\s*[a-zA-Z0-9]+)\\s*,)*\\s*autocommit\\s*=(\\s*(0|1|on|off))\\s*$", Pattern.CASE_INSENSITIVE);
    protected ServerParseValidations serverParseValidations = new ServerParseValidations();

    @Override
    public boolean startWithHint(String stmt) {
        int length = stmt.length();
        for (int i = 0; i < length; ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                    if (i == 0 && stmt.charAt(1) == '*' && stmt.charAt(2) == '!' && stmt.charAt(length - 2) == '*' &&
                            stmt.charAt(length - 1) == '/') {
                        return false;
                    }
                    //fall through
                case '#':
                    i = ParseUtil.commentHint(stmt, i);
                    if (i == -1) {
                        return true;
                    }
                    continue;
                default:
                    break;
            }
            break;
        }
        return false;
    }


    @Override
    public boolean isMultiStatement(String sql) {
        int index = ParseUtil.findNextBreak(sql);
        return index + 1 < sql.length() && !ParseUtil.isEOF(sql, index);
    }

    @Override
    public void getMultiStatement(String sql, LinkedList<String> splitSql) {
        int index = ParseUtil.findNextBreak(sql);
        if (index + 1 < sql.length() && !ParseUtil.isEOF(sql, index)) {
            splitSql.add(sql.substring(0, index).trim());
            sql = sql.substring(index + 1);
            getMultiStatement(sql, splitSql);
        } else {
            if (sql.endsWith(";")) {
                splitSql.add(sql.substring(0, sql.length() - 1).trim());
            } else {
                splitSql.add(sql.trim());
            }
        }
    }

    public static boolean isTCL(int sqlType) {
        switch (sqlType) {
            case ServerParse.BEGIN:
            case ServerParse.START_TRANSACTION:
            case ServerParse.COMMIT:
            case ServerParse.ROLLBACK:
                return true;
            default:
                return false;
        }
    }

    public static boolean isImplicitlyCommitSql(int sqlType) {
        switch (sqlType) {
            case ServerParse.DDL:
            case ServerParse.ALTER_VIEW:
            case ServerParse.CREATE_DATABASE:
            case ServerParse.CREATE_VIEW:
            case ServerParse.DROP_VIEW:
            case ServerParse.DROP_TABLE:
            case RwSplitServerParse.INSTALL:
            case RwSplitServerParse.RENAME:
            case RwSplitServerParse.UNINSTALL:
            case RwSplitServerParse.GRANT:
            case RwSplitServerParse.REVOKE:
            case ServerParse.LOCK:
                return true;
            default:
                return false;
        }
    }

    protected int eCheck(String stmt, int offset) {
        int sqlType = OTHER;
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if (c1 == 'X' || c1 == 'x') {
                switch (c2) {
                    case 'E':
                    case 'e':
                        sqlType = serverParseValidations.executeCheck(stmt, offset);
                        break;
                    case 'P':
                    case 'p':
                        sqlType = serverParseValidations.explainCheck(stmt, offset);
                        break;
                    default:
                        break;
                }
            }
        }
        return sqlType;
    }

    protected int lCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            if (c1 == 'o' || c1 == 'O') {
                switch (stmt.charAt(++offset)) {
                    case 'A':
                    case 'a':
                        return serverParseValidations.loadCheck(stmt, offset);
                    case 'C':
                    case 'c':
                        return serverParseValidations.lockCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    //alter table/view/... and analyze
    protected int aCheck(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case 'l':
                case 'L':
                    return serverParseValidations.alterCheck(stmt, offset);
                case 'n':
                case 'N':
                    return serverParseValidations.analyzeCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    protected int orCheck(String stmt, int offset) {
        int len = stmt.length();
        if (len > ++offset) {
            char c1 = stmt.charAt(offset);
            if ((c1 == 'R' || c1 == 'r')) {
                while (len > ++offset) {
                    if (ParseUtil.isSpace(stmt.charAt(offset))) {
                        continue;
                    }
                    return replaceViewCheck(stmt, offset);
                }
            }
        }
        return DDL;
    }

    private int replaceViewCheck(String stmt, int offset) {
        int len = stmt.length();
        if (len > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'P' || c2 == 'p') && (c3 == 'L' || c3 == 'l') &&
                    (c4 == 'A' || c4 == 'a') && (c5 == 'C' || c5 == 'c') && (c6 == 'E' || c6 == 'e')) {
                while (len > ++offset) {
                    if (ParseUtil.isSpace(stmt.charAt(offset))) {
                        continue;
                    }
                    return serverParseValidations.viewCheck(stmt, offset, true);
                }
            }
        }
        return DDL;
    }

    // KILL' '
    protected int killCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ILL ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'L' || c2 == 'l') &&
                    (c3 == 'L' || c3 == 'l') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            continue;
                        case 'Q':
                        case 'q':
                            return serverParseValidations.killQueryCheck(stmt, offset);
                        case 'c':
                        case 'C':
                            return serverParseValidations.killConnection(stmt, offset);
                        default:
                            return (offset << 8) | KILL;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // DESCRIBE or desc or DELETE' ' or DEALLOCATE' '
    protected int dCheck(String stmt, int offset) {
        int sqlType = OTHER;
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e')) {
                switch (c2) {
                    case 'A':
                    case 'a':
                        sqlType = serverParseValidations.dealCheck(stmt, offset);
                        break;
                    case 'S':
                    case 's':
                        sqlType = serverParseValidations.descCheck(stmt, offset);
                        break;
                    case 'L':
                    case 'l':
                        sqlType = serverParseValidations.deleCheck(stmt, offset);
                        break;
                    default:
                        break;
                }
            }
        }
        return sqlType;
    }

    protected int repCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return serverParseValidations.repair(stmt, offset);
                case 'L':
                case 'l':
                    return serverParseValidations.replace(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    protected int seCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'L':
                case 'l':
                    return serverParseValidations.selectCheck(stmt, offset);
                case 'T':
                case 't':
                    if (stmt.length() > ++offset) {
                        //support QUERY like this
                        //  /*!OBsharding-D: sql=SELECT * FROM test where id=99 */set @pin=1;
                        //  call p_test(@pin,@pout);
                        //  select @pout;
                        if (stmt.startsWith("/*!" + Versions.ANNOTATION_NAME) || stmt.startsWith("/*#" + Versions.ANNOTATION_NAME) || stmt.startsWith("/*" + Versions.ANNOTATION_NAME)) {
                            Matcher matcher = CALL_PATTERN.matcher(stmt);
                            if (matcher.find()) {
                                return CALL;
                            }
                        }

                        char c = stmt.charAt(offset);
                        if (c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '/' || c == '#') {
                            return (offset << 8) | SET;
                        }
                    }
                    return OTHER;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    protected int prepareCheck(String stmt, int offset) {
        if (serverParseValidations.isPrepare(stmt, offset)) return SCRIPT_PREPARE;
        return OTHER;
    }
}
