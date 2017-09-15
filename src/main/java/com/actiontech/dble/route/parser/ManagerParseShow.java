/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseShow {
    private ManagerParseShow() {
    }

    public static final int OTHER = -1;
    public static final int COMMAND = 1;
    public static final int CONNECTION = 2;
    public static final int DATABASE = 3;
    public static final int DATA_NODE = 4;
    public static final int DATASOURCE = 5;
    public static final int HELP = 6;
    public static final int PROCESSOR = 8;
    public static final int SERVER = 10;
    public static final int SQL = 11;
    public static final int SQL_SLOW = 14;
    public static final int SQL_SUM_USER = 15;
    public static final int SQL_SUM_TABLE = 16;
    public static final int SQL_HIGH = 17;
    public static final int SQL_CONDITION = 18;
    public static final int SQL_LARGE = 19;
    public static final int SQL_RESULTSET = 20;

    public static final int THREADPOOL = 21;
    public static final int TIME_CURRENT = 22;
    public static final int TIME_STARTUP = 23;
    public static final int VERSION = 24;
    public static final int CONNECTION_SQL = 27;
    public static final int DATANODE_WHERE = 28;
    public static final int DATASOURCE_WHERE = 29;
    public static final int HEARTBEAT = 30;
    public static final int BACKEND = 33;
    public static final int BACKEND_OLD = 34;

    public static final int CACHE = 35;
    public static final int SESSION = 36;
    public static final int SYSPARAM = 37;
    public static final int SYSLOG = 38;
    public static final int HEARTBEAT_DETAIL = 39;
    public static final int DATASOURCE_SYNC = 40;
    public static final int DATASOURCE_SYNC_DETAIL = 41;
    public static final int DATASOURCE_CLUSTER = 42;

    public static final int WHITE_HOST = 43;
    public static final int WHITE_HOST_SET = 44;
    public static final int DIRECTMEMORY_TOTAL = 45;
    public static final int DIRECTMEMORY_DETAIL = 46;
    public static final int BINLOG_STATUS = 47;

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
                    return show2Check(stmt, i);
                case 'd':
                case 'D':
                    return show2DCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @
    private static int show2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' &&
                stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'B':
                case 'b':
                    return show2BCheck(stmt, offset);
                case 'C':
                case 'c':
                    return show2CCheck(stmt, offset);
                case 'D':
                case 'd':
                    return show2DCheck(stmt, offset);
                case 'H':
                case 'h':
                    return show2HCheck(stmt, offset);
                case 'P':
                case 'p':
                    return show2PCheck(stmt, offset);
                case 'S':
                case 's':
                    return show2SCheck(stmt, offset);
                case 'T':
                case 't':
                    return show2TCheck(stmt, offset);
                case 'V':
                case 'v':
                    return show2VCheck(stmt, offset);
                case 'W':
                case 'w':
                    return show2WCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@B
    private static int show2BCheck(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            switch (c1) {
                case 'A':
                case 'a':
                    return show2BackCheck(stmt, offset);
                case 'I':
                case 'i':
                    return show2BinCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@BINLOG.STATUS
    private static int show2BinCheck(String stmt, int offset) {
        if (stmt.length() > offset + "NLOG.STATUS".length()) {
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
            if ((c2 == 'N' || c2 == 'n') && (c3 == 'L' || c3 == 'l') &&
                    (c4 == 'O' || c4 == 'o') && (c5 == 'G' || c5 == 'g') && (c6 == '.') && (c7 == 'S' || c7 == 's') &&
                    (c8 == 'T' || c8 == 't') && (c9 == 'A' || c9 == 'a') && (c10 == 'T' || c10 == 't') &&
                    (c11 == 'U' || c11 == 'u') && (c12 == 'S' || c12 == 's')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return BINLOG_STATUS;

            }
        }
        return OTHER;
    }

    // SHOW @@BACKEND
    private static int show2BackCheck(String stmt, int offset) {
        if (stmt.length() > offset + "CKEND".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c2 == 'C' || c2 == 'c') && (c3 == 'K' || c3 == 'k') &&
                    (c4 == 'E' || c4 == 'e') && (c5 == 'N' || c5 == 'n') && (c6 == 'D' || c6 == 'd')) {

                if (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ';':
                        case ' ':
                            if (ParseUtil.isErrorTail(offset, stmt)) {
                                return OTHER;
                            }
                            return BACKEND;
                        case '.':
                            return show2BackendOld(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
                return BACKEND;

            }
        }
        return OTHER;
    }

    private static int show2BackendOld(String stmt, int offset) {
        if (stmt.length() > offset + "OLD".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'L' || c2 == 'l') && (c3 == 'D' || c3 == 'd')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return BACKEND_OLD;
            }
        }
        return OTHER;
    }

    // SHOW @@C
    private static int show2CCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'O':
                case 'o':
                    return show2CoCheck(stmt, offset);
                case 'A':
                case 'a':
                    return show2CACheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@CACHE
    private static int show2CACheck(String stmt, int offset) {
        String remain = stmt.substring(offset);
        if (remain.equalsIgnoreCase("ACHE")) {
            return CACHE;
        }
        return OTHER;
    }

    // SHOW @@DATA
    private static int show2DCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ATA".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a') &&
                    stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                    case 'B':
                    case 'b':
                        return show2DataBCheck(stmt, offset);
                    case 'N':
                    case 'n':
                        return show2DataNCheck(stmt, offset);
                    case 'S':
                    case 's':
                        return show2DataSCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            } else if ((c1 == 'I' || c1 == 'i') &&
                    (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'E' || c3 == 'e') &&
                    stmt.length() > ++offset) {   /**DIRECTMEMORY**/
                switch (stmt.charAt(offset)) {
                    case 'C':
                    case 'c':
                        return show2DirectMemoryCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW @@DIRECT_MEMORY=1 or 0
    private static int show2DirectMemoryCheck(String stmt, int offset) {
        int returnValue = OTHER;
        if (stmt.length() > offset + "TMEMORY".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            if ((c1 == 'T' || c1 == 't') &&
                    (c2 == 'M' || c2 == 'm') &&
                    (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'M' || c4 == 'm') &&
                    (c5 == 'O' || c5 == 'o') &&
                    (c6 == 'R' || c6 == 'r') &&
                    (c7 == 'Y' || c7 == 'y') &&
                    stmt.length() > ++offset) {

                offset = trim(offset, stmt);

                if (stmt.charAt(offset) != '=') {
                    return OTHER;
                } else {
                    offset++;
                }
                offset = trim(offset, stmt);
                switch (stmt.charAt(offset)) {
                    case '1':
                        returnValue = DIRECTMEMORY_TOTAL;
                        break;
                    case '2':
                        returnValue = DIRECTMEMORY_DETAIL;
                        break;
                    default:
                        return OTHER;
                }

                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }

            }
        }

        return returnValue;
    }

    // SHOW @@DataSyn
    private static int show2DataSynCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'S':
                case 's':
                    if (stmt.length() > offset + "yn".length()) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        if ((c1 == 'Y' || c1 == 'y') && (c2 == 'N' || c2 == 'n')) {
                            switch (stmt.charAt(++offset)) {
                                case 'S':
                                case 's':
                                    return show2SynStatuslCheck(stmt, offset);
                                case 'D':
                                case 'd':
                                    return show2SynDetailCheck(stmt, offset);
                                default:
                                    return OTHER;
                            }

                        } else {
                            return OTHER;
                        }
                    }
                    return OTHER;
                case 'C':
                case 'c':
                    if (stmt.length() > offset + "luster".length()) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        char c3 = stmt.charAt(++offset);
                        char c4 = stmt.charAt(++offset);
                        char c5 = stmt.charAt(++offset);
                        char c6 = stmt.charAt(++offset);
                        if ((c1 == 'L' || c1 == 'l') && (c2 == 'U' || c2 == 'u') &&
                                (c3 == 'S' || c3 == 's') && (c4 == 'T' || c4 == 't') &&
                                (c5 == 'E' || c5 == 'e') && (c6 == 'R' || c6 == 'r')) {
                            return DATASOURCE_CLUSTER;
                        } else {
                            return OTHER;
                        }
                    }
                    return OTHER;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    //show @@datasource.syndetail
    private static int show2SynDetailCheck(String stmt, int offset) {
        if (stmt.length() > offset + "etail where name=".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'L' || c5 == 'l') && c6 == ' ') {
                offset = trim(++offset, stmt);
                if (offset >= stmt.length() - 1) {
                    return OTHER;
                }
                return synDetailWhereCheck(stmt, offset);
            }
        }
        return OTHER;
    }

    private static int synDetailWhereCheck(String stmt, int offset) {
        char c11 = stmt.charAt(offset);
        char c21 = stmt.charAt(++offset);
        char c31 = stmt.charAt(++offset);
        char c41 = stmt.charAt(++offset);
        char c51 = stmt.charAt(++offset);
        char c61 = stmt.charAt(++offset);
        if ((c11 == 'W' || c11 == 'w') && (c21 == 'H' || c21 == 'h') && (c31 == 'E' || c31 == 'e') &&
                (c41 == 'R' || c41 == 'r') && (c51 == 'E' || c51 == 'e') && c61 == ' ') {
            offset = trim(++offset, stmt);
            if (offset >= stmt.length() - 1) {
                return OTHER;
            }
            char c12 = stmt.charAt(offset);
            char c22 = stmt.charAt(++offset);
            char c32 = stmt.charAt(++offset);
            char c42 = stmt.charAt(++offset);
            if ((c12 == 'N' || c12 == 'n') && (c22 == 'A' || c22 == 'a') && (c32 == 'M' || c32 == 'm') &&
                    (c42 == 'e' || c42 == 'E')) {
                offset = trim(++offset, stmt);
                if (offset >= stmt.length() - 1) {
                    return OTHER;
                }
                if (stmt.charAt(offset) == '=') {
                    offset = trim(++offset, stmt);
                    if (offset >= stmt.length() - 1) {
                        return OTHER;
                    }
                    String name = stmt.substring(offset).trim();
                    if (name.length() > 0 && !name.contains(" ")) {
                        return DATASOURCE_SYNC_DETAIL;
                    }
                }
            }
        }
        //if (ParseUtil.isErrorTail(++offset, stmt)) {
        //    return OTHER;
        //}
        return OTHER;
    }

    //show @@datasource.synstatus
    private static int show2SynStatuslCheck(String stmt, int offset) {
        if (stmt.length() > offset + "tatus".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);

            if ((c1 == 'T' || c1 == 't') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't') &&
                    (c4 == 'U' || c4 == 'u') && (c5 == 'S' || c5 == 's')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return DATASOURCE_SYNC;
            }
        }
        return OTHER;
    }

    // SHOW @@HELP
    private static int show2HCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return show2HeCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@HE
    private static int show2HeCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'L':
                case 'l':
                    return show2HelCheck(stmt, offset);
                case 'A':
                case 'a':
                    return ManagerParseHeartbeat.show2HeaCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@HELP
    private static int show2HelCheck(String stmt, int offset) {
        if (stmt.length() > offset + "P".length()) {
            char c1 = stmt.charAt(++offset);
            if ((c1 == 'P' || c1 == 'p')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return HELP;
            }
        }
        return OTHER;
    }


    // SHOW @@P
    private static int show2PCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'R':
                case 'r':
                    return show2PrCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@S
    private static int show2SCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return show2SeCheck(stmt, offset);
                case 'Q':
                case 'q':
                    return show2SqCheck(stmt, offset);
                case 'Y':
                case 'y':
                    return show2SyCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@SYSPARAM
    private static int show2sysparam(String stmt, int offset) {
        if (stmt.length() > offset + "ARAM".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);

            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'A' || c3 == 'a') && (c4 == 'M' || c4 == 'm')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return SYSPARAM;
            }
        }
        return OTHER;
    }

    private static int show2syslog(String stmt, int offset) {

        if (stmt.length() > offset + "SLOG".length()) {

            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);

            if ((c1 == 'O' || c1 == 'o') && (c2 == 'G' || c2 == 'g') && c3 == ' ') {

                offset = trim(offset, stmt);

                char c4 = stmt.charAt(offset);
                char c5 = stmt.charAt(++offset);
                char c6 = stmt.charAt(++offset);
                char c7 = stmt.charAt(++offset);
                char c8 = stmt.charAt(++offset);

                if ((c4 == 'L' || c4 == 'l') && (c5 == 'I' || c5 == 'i') && (c6 == 'M' || c6 == 'm') &&
                        (c7 == 'I' || c7 == 'i') && (c8 == 'T' || c8 == 't')) {

                    while (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                            case ' ':
                                continue;
                            case '=':
                                while (stmt.length() > ++offset) {
                                    switch (stmt.charAt(offset)) {
                                        case ' ':
                                            continue;
                                        default:
                                            return (offset << 8) | SYSLOG;
                                    }
                                }
                                return OTHER;
                            default:
                                return OTHER;
                        }
                    }
                }

                return SYSLOG;
            }
        }

        return OTHER;
    }

    // SHOW @@SYSPARAM
    // SHOW @@SYSLOG LIMIT=1000
    private static int show2SyCheck(String stmt, int offset) {

        if (stmt.length() > offset + "YS".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if (c1 == 'S' || c1 == 's') {
                switch (c2) {
                    case 'L':
                    case 'l':
                        return show2syslog(stmt, offset);
                    case 'P':
                    case 'p':
                        return show2sysparam(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW @@T
    private static int show2TCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'H':
                case 'h':
                    return show2ThCheck(stmt, offset);
                case 'I':
                case 'i':
                    return show2TiCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@VERSION
    private static int show2VCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ERSION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'S' || c3 == 's') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return VERSION;
            }
        }
        return OTHER;
    }

    // SHOW @@White  ip white list
    private static int show2WCheck(String stmt, int offset) {
        if (stmt.length() > offset + "HITE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'I' || c2 == 'i') && (c3 == 'T' || c3 == 't') &&
                    (c4 == 'E' || c4 == 'e')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return WHITE_HOST;
            }
        }
        return OTHER;
    }

    // SHOW @@CO
    private static int show2CoCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'M':
                case 'm':
                    return show2ComCheck(stmt, offset);
                case 'N':
                case 'n':
                    return show2ConCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@DATABASE
    private static int show2DataBCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ASE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'S' || c2 == 's') && (c3 == 'E' || c3 == 'e')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return DATABASE;
            }
        }
        return OTHER;
    }

    // SHOW @@DATANODE
    private static int show2DataNCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ODE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'D' || c2 == 'd') && (c3 == 'E' || c3 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'W':
                        case 'w':
                            if (stmt.charAt(offset - 1) != ' ') {
                                return OTHER;
                            }
                            return show2DataNWhereCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
                return DATA_NODE;
            }
        }
        return OTHER;
    }

    // SHOW @@DATANODE WHERE
    private static int show2DataNWhereCheck(String stmt, int offset) {
        if (stmt.length() > offset + "HERE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'E' || c4 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'S':
                        case 's':
                            if (stmt.charAt(offset - 1) != ' ') {
                                return OTHER;
                            }
                            return show2DataNWhereSchemaCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW @@DATANODE WHERE SCHEMA = XXXXXX
    private static int show2DataNWhereSchemaCheck(String stmt, int offset) {
        if (stmt.length() > offset + "CHEMA".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'C' || c1 == 'c') && (c2 == 'H' || c2 == 'h') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'M' || c4 == 'm') && (c5 == 'A' || c5 == 'a')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '=':
                            while (stmt.length() > ++offset) {
                                switch (stmt.charAt(offset)) {
                                    case ' ':
                                        continue;
                                    default:
                                        return (offset << 8) | DATANODE_WHERE;
                                }
                            }
                            return OTHER;
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW @@DATASOURCE
    private static int show2DataSCheck(String stmt, int offset) {
        if (stmt.length() > offset + "OURCE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'U' || c2 == 'u') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'C' || c4 == 'c') && (c5 == 'E' || c5 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'W':
                        case 'w':
                            if (stmt.charAt(offset - 1) != ' ') {
                                return OTHER;
                            }
                            return show2DataSWhereCheck(stmt, offset);
                        case '.':
                            return show2DataSynCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }

                return DATASOURCE;
            }
        }
        return OTHER;
    }

    // SHOW @@DATASOURCE WHERE
    private static int show2DataSWhereCheck(String stmt, int offset) {
        if (stmt.length() > offset + "HERE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'E' || c4 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'd':
                        case 'D':
                            if (stmt.charAt(offset - 1) != ' ') {
                                return OTHER;
                            }
                            return show2DataSWhereDatanodeCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW @@DATASOURCE WHERE DATANODE = XXXXXX
    private static int show2DataSWhereDatanodeCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ATANODE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a') &&
                    (c4 == 'N' || c4 == 'n') && (c5 == 'O' || c5 == 'o') && (c6 == 'D' || c6 == 'd') &&
                    (c7 == 'E' || c7 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '=':
                            while (stmt.length() > ++offset) {
                                switch (stmt.charAt(offset)) {
                                    case ' ':
                                        continue;
                                    default:
                                        return (offset << 8) | DATASOURCE_WHERE;
                                }
                            }
                            return OTHER;
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW @@PROCESSOR
    private static int show2PrCheck(String stmt, int offset) {
        if (stmt.length() > offset + "OCESSOR".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'C' || c2 == 'c') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'S' || c4 == 's') && (c5 == 'S' || c5 == 's') && (c6 == 'O' || c6 == 'o') &&
                    (c7 == 'R' || c7 == 'r')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return PROCESSOR;
            }
        }
        return OTHER;
    }

    // SHOW @@SERVER
    // SHOW @@SESSION
    private static int show2SeCheck(String stmt, int offset) {
        if (stmt.length() > offset + "SSION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'S' || c2 == 's') && (c3 == 'I' || c3 == 'i') &&
                    (c4 == 'O' || c4 == 'o') && (c5 == 'N' || c5 == 'n')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return SESSION;
            }
        }
        if (stmt.length() > offset + "RVER".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'V' || c2 == 'v') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'R' || c4 == 'r')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return SERVER;
            }
        }
        return OTHER;
    }

    // SHOW @@THREADPOOL
    private static int show2ThCheck(String stmt, int offset) {
        if (stmt.length() > offset + "READPOOL".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'E' || c2 == 'e') && (c3 == 'A' || c3 == 'a') &&
                    (c4 == 'D' || c4 == 'd') && (c5 == 'P' || c5 == 'p') && (c6 == 'O' || c6 == 'o') &&
                    (c7 == 'O' || c7 == 'o') && (c8 == 'L' || c8 == 'l')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return THREADPOOL;
            }
        }
        return OTHER;
    }

    // SHOW @@TIME.
    private static int show2TiCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ME.".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'M' || c1 == 'm') && (c2 == 'E' || c2 == 'e') && (c3 == '.') &&
                    (stmt.length() > ++offset)) {
                switch (stmt.charAt(offset)) {
                    case 'C':
                    case 'c':
                        return show2TimeCCheck(stmt, offset);
                    case 'S':
                    case 's':
                        return show2TimeSCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW @@COMMAND
    private static int show2ComCheck(String stmt, int offset) {
        if (stmt.length() > offset + "MAND".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'M' || c1 == 'm') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'D' || c4 == 'd')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return COMMAND;
            }
        }
        return OTHER;
    }

    // SHOW @@CONNECTION
    private static int show2ConCheck(String stmt, int offset) {
        if (stmt.length() > offset + "NECTION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'E' || c2 == 'e') && (c3 == 'C' || c3 == 'c') &&
                    (c4 == 'T' || c4 == 't') && (c5 == 'I' || c5 == 'i') && (c6 == 'O' || c6 == 'o') &&
                    (c7 == 'N' || c7 == 'n')) {
                if (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            if (ParseUtil.isErrorTail(offset, stmt)) {
                                return OTHER;
                            }
                            return CONNECTION;
                        case '.':
                            return show2ConnectonSQL(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
                return CONNECTION;
            }
        }
        return OTHER;
    }

    // SHOW @@CONNECTION.SQL
    private static int show2ConnectonSQL(String stmt, int offset) {
        if (stmt.length() > offset + "SQL".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'Q' || c2 == 'q') && (c3 == 'L' || c3 == 'l')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return CONNECTION_SQL;
            }
        }
        return OTHER;
    }

    // SHOW @@TIME.CURRENT
    private static int show2TimeCCheck(String stmt, int offset) {
        if (stmt.length() > offset + "URRENT".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'R' || c2 == 'r') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'E' || c4 == 'e') && (c5 == 'N' || c5 == 'n') && (c6 == 'T' || c6 == 't')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return TIME_CURRENT;
            }
        }
        return OTHER;
    }

    // SHOW @@TIME.STARTUP
    private static int show2TimeSCheck(String stmt, int offset) {
        if (stmt.length() > offset + "TARTUP".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'T' || c1 == 't') && (c2 == 'A' || c2 == 'a') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'T' || c4 == 't') && (c5 == 'U' || c5 == 'u') && (c6 == 'P' || c6 == 'p')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return TIME_STARTUP;
            }
        }
        return OTHER;
    }

    // SHOW @@SQ
    private static int show2SqCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'L':
                case 'l':
                    return show2SqlCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@SQL
    private static int show2SqlCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case '.':
                    return show2SqlDotCheck(stmt, offset);
                case ' ':
                    return show2SqlBlankCheck(stmt, offset);
                default:
                    if (ParseUtil.isErrorTail(offset, stmt)) {
                        return OTHER;
                    }
                    return SQL;
            }
        } else {
            return SQL;
        }
    }

    // SHOW @@SQL.
    private static int show2SqlDotCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'S':
                case 's':
                    char c1 = stmt.charAt(++offset);
                    switch (c1) {
                        case 'L':
                        case 'l':
                            return show2SqlSLCheck(stmt, offset);
                        case 'U':
                        case 'u':
                            return show2SqlSUCheck(stmt, offset);
                        default:
                            break;
                    }
                    return OTHER;
                case 'H':
                case 'h':
                    return show2SqlHCheck(stmt, offset);
                case 'L':
                case 'l':
                    return show2SqlLCheck(stmt, offset);
                case 'C':
                case 'c':
                    return show2SqlCCheck(stmt, offset);
                case 'R':
                case 'r':
                    return show2SqlRCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW @@SQL WHERE ID = XXXXXX
    private static int show2SqlBlankCheck(String stmt, int offset) {
        for (++offset; stmt.length() > offset; ) {
            switch (stmt.charAt(offset)) {
                case ' ':
                    continue;
                case 'W':
                case 'w':
                    if (isWhere(stmt, offset)) {
                        return SQL;
                    } else {
                        return OTHER;
                    }
                default:
                    if (!isBoolean(stmt.substring(offset))) {
                        return OTHER;
                    }
                    return (offset << 8) | SQL;
            }
        }

        return OTHER;
    }

    // SHOW @@SQL.SLOW
    private static int show2SqlSLCheck(String stmt, int offset) {
        if (stmt.length() > offset + "OW".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w')) {

                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        default:
                            if (!isBoolean(stmt.substring(offset).trim())) {
                                return OTHER;
                            }
                            return (offset << 8) | SQL_SLOW;
                    }
                }

                return SQL_SLOW;
            }
        }
        return OTHER;
    }

    // SHOW @@SQL.HIGH
    private static int show2SqlHCheck(String stmt, int offset) {

        if (stmt.length() > offset + "IGH".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'G' || c2 == 'g') && (c3 == 'H' || c3 == 'h')) {
                return boolTailCheck(stmt, offset, SQL_HIGH);
            }
        }
        return OTHER;
    }

    // SHOW @@SQL.RESULTSET
    private static int show2SqlRCheck(String stmt, int offset) {

        if (stmt.length() > offset + "ESULTSET".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') && (c3 == 'U' || c3 == 'u') &&
                    (c4 == 'l' || c4 == 'L') && (c5 == 'T' || c5 == 't') && (c6 == 'S' || c6 == 's') &&
                    (c7 == 'E' || c7 == 'e') && (c8 == 'T' || c8 == 't')) {
                return boolTailCheck(stmt, offset, SQL_RESULTSET);
            }
        }
        return OTHER;
    }

    // SHOW @@SQL.LARGE
    private static int show2SqlLCheck(String stmt, int offset) {

        if (stmt.length() > offset + "ARGE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') && (c3 == 'G' || c3 == 'g') && (c4 == 'E' || c4 == 'e')) {
                return boolTailCheck(stmt, offset, SQL_LARGE);
            }
        }
        return OTHER;
    }

    // SHOW @@sql.condition
    private static int show2SqlCCheck(String stmt, int offset) {

        if (stmt.length() > offset + "ONDITION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'D' || c3 == 'd') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i') &&
                    (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n')) {
                if (ParseUtil.isErrorTail(++offset, stmt)) {
                    return OTHER;
                }
                return SQL_CONDITION;
            }
        }
        return OTHER;
    }

    // SHOW @@SQL.SUM
    private static int show2SqlSUCheck(String stmt, int offset) {
        if (stmt.length() > offset + "M".length()) {
            char c1 = stmt.charAt(++offset);
            if (c1 == 'M' || c1 == 'm') {
                if (stmt.length() > ++offset && stmt.charAt(offset) == '.') {

                    /*
                     *  keep SHOW @@SQL.SUM
                     *  add  SHOW @@SQL.SUM.TABLE , SHOW @@SQL.SUM.USER
                     *  SHOW @@SQL.SUM == SHOW @@SQL.SUM.TABLE
                     */
                    if (stmt.length() > (offset + 4)) {
                        char c2 = stmt.charAt(++offset);
                        char c3 = stmt.charAt(++offset);
                        char c4 = stmt.charAt(++offset);
                        char c5 = stmt.charAt(++offset);

                        if ((c2 == 'U' || c2 == 'u') && (c3 == 'S' || c3 == 's') &&
                                (c4 == 'E' || c4 == 'e') && (c5 == 'R' || c5 == 'r')) {
                            return boolTailCheck(stmt, offset, SQL_SUM_USER);

                        } else if ((c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a') &&
                                (c4 == 'B' || c4 == 'b') && (c5 == 'L' || c5 == 'l') &&
                                stmt.length() > (offset + 1)) {

                            char c6 = stmt.charAt(++offset);
                            if (c6 == 'E' || c6 == 'e') {
                                return boolTailCheck(stmt, offset, SQL_SUM_TABLE);
                            }
                        }

                    }

                    return OTHER;
                }
                return boolTailCheck(stmt, offset, SQL_SUM_USER);
            }
        }
        return OTHER;
    }

    private static int boolTailCheck(String stmt, int offset, int result) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                    continue;
                default:
                    if (!isBoolean(stmt.substring(offset))) {
                        return OTHER;
                    }
                    return (offset << 8) | result;
            }
        }

        return result;
    }


    private static boolean isWhere(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'E' || c4 == 'e') && (c5 == ' ')) {
                boolean jump1 = false;
                for (++offset; stmt.length() > offset && !jump1; ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'I':
                        case 'i':
                            jump1 = true;
                            break;
                        default:
                            return false;
                    }
                }
                if ((stmt.length() > offset) && (stmt.charAt(offset) == 'D' || stmt.charAt(offset) == 'd')) {
                    boolean jump2 = false;
                    for (++offset; stmt.length() > offset && !jump2; ++offset) {
                        switch (stmt.charAt(offset)) {
                            case ' ':
                                continue;
                            case '=':
                                jump2 = true;
                                break;
                            default:
                                return false;
                        }
                    }
                    return isSqlId(stmt, offset);
                }
            }
        }
        return false;
    }

    private static boolean isSqlId(String stmt, int offset) {
        String id = stmt.substring(offset).trim();
        try {
            Long.parseLong(id);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static String getWhereParameter(String stmt) {
        int offset = stmt.indexOf('=');
        ++offset;
        return stmt.substring(offset).trim();
    }


    /**
     * skip all whitespace ,return the index of last whitespace
     *
     * @param offset
     * @param stmt
     * @return
     */
    public static int trim(int offset, String stmt) {
        for (; offset < stmt.length(); offset++) {
            if (!ParseUtil.isSpace(stmt.charAt(offset))) {
                return offset;
            }
        }
        return offset;
    }


    public static boolean isBoolean(String str) {
        return "false".equalsIgnoreCase(str) || "true".equalsIgnoreCase(str);
    }
}
