/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 * @author mycat
 */
public final class ManagerParse {
    private ManagerParse() {
    }

    public static final int OTHER = -1;
    public static final int SELECT = 1;
    public static final int SET = 2;
    public static final int SHOW = 3;
    public static final int DESCRIBE = 4;
    public static final int KILL_CONN = 5;
    public static final int STOP = 6;
    public static final int RELOAD = 7;
    public static final int OFFLINE = 9;
    public static final int ONLINE = 10;
    public static final int CHECK = 11;
    public static final int CONFIGFILE = 12;
    public static final int LOGFILE = 13;
    public static final int PAUSE = 14;
    public static final int RESUME = 15;
    public static final int CREATE_DB = 16;
    public static final int DRY_RUN = 17;
    public static final int ENABLE = 18;
    public static final int DISABLE = 19;
    public static final int KILL_DDL_LOCK = 20;
    public static final int KILL_XA_SESSION = 21;
    public static final int RELEASE_RELOAD_METADATA = 22;
    public static final int DB_GROUP = 23;
    public static final int SPLIT = 24;
    public static final int DROP_DB = 25;
    public static final int FLOW_CONTROL = 26;
    public static final int INSERT = 27;
    public static final int DELETE = 28;
    public static final int UPDATE = 29;
    public static final int FRESH_CONN = 30;
    public static final int USE = 31;

    public static int parse(String stmt) {
        for (int i = 0; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case 'C':
                case 'c':
                    return cCheck(stmt, i);
                case 'D':
                case 'd':
                    return dCheck(stmt, i);
                case 'E':
                case 'e':
                    return eCheck(stmt);
                case 'F':
                case 'f':
                    return fCheck(stmt, i);
                case 'L':
                case 'l':
                    return lCheck(stmt, i);
                case 'S':
                case 's':
                    return sCheck(stmt, i);
                case 'K':
                case 'k':
                    return kill(stmt, i);
                case 'O':
                case 'o':
                    return oCheck(stmt, i);
                case 'R':
                case 'r':
                    return rCheck(stmt, i);
                case 'P':
                case 'p':
                    return pCheck(stmt, i);
                case 'I':
                case 'i':
                    return insert(stmt, i);
                case 'U':
                case 'u':
                    return uCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // show LOG check
    private static int lCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("LOG @@")) {
            return LOGFILE;
        } else {
            return OTHER;
        }
    }

    private static int dCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'B':
                case 'b':
                    return dbGroupCheck(stmt, offset);
                case 'R':
                case 'r':
                    return drCheck(stmt, offset);
                case 'I':
                case 'i':
                    return disCheck(stmt);
                case 'E':
                case 'e':
                    return descCheck(stmt);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int dbGroupCheck(String stmt, int offset) {
        if (stmt.length() > offset + "Group".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'g' || c1 == 'G') &&
                    (c2 == 'r' || c2 == 'R') &&
                    (c3 == 'o' || c3 == 'O') &&
                    (c4 == 'u' || c4 == 'U') &&
                    (c5 == 'p' || c5 == 'P')) {
                return DB_GROUP;
            }
        }
        return OTHER;
    }

    private static int drCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'o':
                case 'O':
                    return dropCheck(stmt, offset);
                case 'y':
                case 'Y':
                    return dryRunCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int dryRunCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("YRUN")) {
            return DRY_RUN;
        } else {
            return OTHER;
        }
    }

    private static int dropCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("OP")) {
            return DROP_DB;
        } else {
            return OTHER;
        }
    }

    private static int disCheck(String stmt) {
        String thePart = stmt.toUpperCase();
        if (thePart.startsWith("DISABLE") && stmt.length() > 7 && ParseUtil.isSpace(stmt.charAt(7))) {
            return (8 << 8) | DISABLE;
        }
        return OTHER;
    }

    private static int descCheck(String stmt) {
        String thePart = stmt.toUpperCase();
        if (thePart.startsWith("DESCRIBE") && stmt.length() > 8 && ParseUtil.isSpace(stmt.charAt(8))) {
            return (9 << 8) | DESCRIBE;
        } else if (thePart.startsWith("DESC") && stmt.length() > 4 && ParseUtil.isSpace(stmt.charAt(4))) {
            return (5 << 8) | DESCRIBE;
        } else if (thePart.startsWith("DELETE") && stmt.length() > 6 && ParseUtil.isSpace(stmt.charAt(6))) {
            return DELETE;
        }
        return OTHER;
    }

    private static int eCheck(String stmt) {
        String thePart = stmt.toUpperCase();
        if (thePart.startsWith("ENABLE") && stmt.length() > 6 && ParseUtil.isSpace(stmt.charAt(6))) {
            return (7 << 8) | ENABLE;
        }
        return OTHER;
    }

    // config file check
    private static int fCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("FILE @@")) {
            return CONFIGFILE;
        }
        if (stmt.length() <= ++offset) {
            return OTHER;
        }
        char c1 = stmt.charAt(offset);
        switch (c1) {
            case 'L':
            case 'l':
                return flCheck(stmt, offset);
            case 'R':
            case 'r':
                return frCheck(stmt, offset);
            default:
                return OTHER;
        }
    }

    private static int flCheck(String stmt, int offset) {
        if (stmt.length() > offset + "OW_CONTROL".length()) {
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
            if ((c2 == 'O' || c2 == 'o') &&
                    (c3 == 'W' || c3 == 'w') &&
                    (c4 == '_') &&
                    (c5 == 'C' || c5 == 'c') &&
                    (c6 == 'O' || c6 == 'o') &&
                    (c7 == 'N' || c7 == 'n') &&
                    (c8 == 'T' || c8 == 't') &&
                    (c9 == 'R' || c9 == 'r') &&
                    (c10 == 'O' || c10 == 'o') &&
                    (c11 == 'L' || c11 == 'l')) {
                return FLOW_CONTROL;
            }
        }
        return OTHER;
    }


    private static int frCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') &&
                    (c3 == 'H' || c3 == 'h') && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return freshCheck(stmt, offset);
            }
        }
        return OTHER;
    }

    private static int freshCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'C' || c1 == 'c') && (c2 == 'O' || c2 == 'o') &&
                    (c3 == 'N' || c3 == 'n') && (c4 == 'N' || c4 == 'n') && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return FRESH_CONN;
            }
        }
        return OTHER;
    }


    private static int cCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'R':
                case 'r':
                    return crCheck(stmt, offset);
                case 'H':
                case 'h':
                    return chCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int chCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("HECK")) {
            return (6 << 8) | CHECK;
        }
        return OTHER;
    }

    private static int crCheck(String stmt, int offset) {
        String thePart = stmt.substring(offset).toUpperCase();
        if (thePart.startsWith("REATE")) {
            return CREATE_DB;
        }
        return OTHER;
    }

    private static int oCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'F':
                case 'f':
                    return ofCheck(stmt, offset);
                case 'N':
                case 'n':
                    return onCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int onCheck(String stmt, int offset) {
        if (stmt.length() > offset + "line".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'l' || c1 == 'L') &&
                    (c2 == 'i' || c2 == 'I') &&
                    (c3 == 'n' || c3 == 'N') &&
                    (c4 == 'e' || c4 == 'E') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return ONLINE;
            }
        }
        return OTHER;
    }

    private static int ofCheck(String stmt, int offset) {
        if (stmt.length() > offset + "fline".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'f' || c1 == 'F') &&
                    (c2 == 'l' || c2 == 'L') &&
                    (c3 == 'i' || c3 == 'I') &&
                    (c4 == 'n' || c4 == 'N') &&
                    (c5 == 'e' || c5 == 'E') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return OFFLINE;
            }
        }
        return OTHER;
    }

    private static int sCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return seCheck(stmt, offset);
                case 'H':
                case 'h':
                    return show(stmt, offset);
                case 'P':
                case 'p':
                    return split(stmt, offset);
                case 'T':
                case 't':
                    return stop(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int seCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'L':
                case 'l':
                    return select(stmt, offset);
                case 'T':
                case 't':
                    if (stmt.length() > ++offset) {
                        char c = stmt.charAt(offset);
                        if (c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '/' || c == '#') {
                            return SET;
                        }
                    }
                    return OTHER;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int rCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return reCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int reCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'S':
                case 's':
                    return resume(stmt, offset);
                case 'L':
                case 'l':
                    return relCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int relCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'O':
                case 'o':
                    return reload(stmt, offset);
                case 'E':
                case 'e':
                    return release(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    //RELEASE
    private static int release(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c2 == 'A' || c2 == 'a') &&
                    (c3 == 'S' || c3 == 's') && (c4 == 'E' || c4 == 'e') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                while (true) {
                    switch (stmt.charAt(++offset)) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case '@':
                            return releaseReloadMetadata(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    private static int releaseReloadMetadata(String stmt, int offset) {
        char c11 = stmt.charAt(++offset);
        char c12 = stmt.charAt(++offset);
        char c13 = stmt.charAt(++offset);
        char c14 = stmt.charAt(++offset);
        char c15 = stmt.charAt(++offset);
        char c16 = stmt.charAt(++offset);
        char c17 = stmt.charAt(++offset);
        char c18 = stmt.charAt(++offset);
        char c19 = stmt.charAt(++offset);
        char c1a = stmt.charAt(++offset);
        char c1b = stmt.charAt(++offset);
        char c1c = stmt.charAt(++offset);
        char c1d = stmt.charAt(++offset);
        char c1e = stmt.charAt(++offset);
        char c1f = stmt.charAt(++offset);
        char c1g = stmt.charAt(++offset);
        if (c11 == '@' && (c12 == 'R' || c12 == 'r') &&
                (c13 == 'E' || c13 == 'e') &&
                (c14 == 'L' || c14 == 'l') &&
                (c15 == 'O' || c15 == 'o') &&
                (c16 == 'A' || c16 == 'a') &&
                (c17 == 'D' || c17 == 'd') && c18 == '_' &&
                (c19 == 'M' || c19 == 'm') &&
                (c1a == 'E' || c1a == 'e') &&
                (c1b == 'T' || c1b == 't') &&
                (c1c == 'A' || c1c == 'a') &&
                (c1d == 'D' || c1d == 'd') &&
                (c1e == 'A' || c1e == 'a') &&
                (c1f == 'T' || c1f == 't') &&
                (c1g == 'A' || c1g == 'a') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
            return RELEASE_RELOAD_METADATA;
        }
        return OTHER;
    }

    //RESUME
    private static int resume(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') &&
                    (c2 == 'm' || c2 == 'M') && (c3 == 'e' || c3 == 'E') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return RESUME;
            }
        }
        return OTHER;
    }

    // RELOAD' '
    private static int reload(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c3 == 'A' || c3 == 'a') && (c4 == 'D' || c4 == 'd') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return (offset << 8) | RELOAD;
            }
        }
        return OTHER;
    }


    private static int pCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return pause(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }


    private static int pause(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'u' || c1 == 'U') && (c2 == 'S' || c2 == 's') &&
                    (c3 == 'E' || c3 == 'e') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return PAUSE;
            }
        }
        return OTHER;
    }

    // SELECT' '
    private static int select(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'C' || c2 == 'c') && (c3 == 'T' || c3 == 't') &&
                    ParseUtil.isSpace(c4)) {
                return (offset << 8) | SELECT;
            }
        }
        return OTHER;
    }

    // SHOW' '
    private static int show(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w') &&
                    (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
                return (offset << 8) | SHOW;
            }
        }
        return OTHER;
    }

    private static int insert(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's') &&
                    (c3 == 'E' || c3 == 'e') && (c4 == 'R' || c4 == 'r') && (c5 == 'T' || c5 == 't') &&
                    ParseUtil.isSpace(c6)) {
                return INSERT;
            }
        }
        return OTHER;
    }


    // SPLIT ' '
    private static int split(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'I' || c2 == 'i') &&
                    (c3 == 'T' || c3 == 't') && ParseUtil.isSpace(c4)) {
                return (offset << 8) | SPLIT;
            }
        }
        return OTHER;
    }

    // STOP' '
    private static int stop(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'P' || c2 == 'p') &&
                    (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
                return (offset << 8) | STOP;
            }
        }
        return OTHER;
    }

    // KILL @
    private static int kill(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
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
                        case '@':
                            return killCheck(stmt, offset);
                        default:
                            return OTHER;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // KILL @@
    private static int killCheck(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return killConnection(stmt, offset);
                case 'D':
                case 'd':
                    return killDdl(stmt, offset);
                case 'X':
                case 'x':
                    return killXASession(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // KILL @@DDL_LOCK WHERE SCHEMA=? AND TABLE=?
    private static int killDdl(String stmt, int offset) {
        if (stmt.length() > offset + "DL_LOCK ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            if ((c1 == 'D' || c1 == 'd') &&
                    (c2 == 'L' || c2 == 'l') &&
                    (c3 == '_') &&
                    (c4 == 'L' || c4 == 'l') &&
                    (c5 == 'O' || c5 == 'o') &&
                    (c6 == 'C' || c6 == 'c') &&
                    (c7 == 'K' || c7 == 'k')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case 'W':
                        case 'w':
                            char c8 = stmt.charAt(++offset);
                            char c9 = stmt.charAt(++offset);
                            char c10 = stmt.charAt(++offset);
                            char c11 = stmt.charAt(++offset);
                            char c12 = stmt.charAt(++offset);
                            if ((c8 == 'H' || c8 == 'h') &&
                                    (c9 == 'E' || c9 == 'e') &&
                                    (c10 == 'R' || c10 == 'r') &&
                                    (c11 == 'E' || c11 == 'e') &&
                                    (c12 == ' ')) {
                                return (offset << 8) | KILL_DDL_LOCK;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return OTHER;
    }

    // KILL @@CONNECTION' ' XXXXXX
    private static int killConnection(String stmt, int offset) {
        if (stmt.length() > offset + "ONNECTION ".length()) {
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
            if ((c1 == 'O' || c1 == 'o') &&
                    (c2 == 'N' || c2 == 'n') &&
                    (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'E' || c4 == 'e') &&
                    (c5 == 'C' || c5 == 'c') &&
                    (c6 == 'T' || c6 == 't') &&
                    (c7 == 'I' || c7 == 'i') &&
                    (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'N' || c9 == 'n') &&
                    (c10 == ' ' || c10 == '\t' || c10 == '\r' || c10 == '\n')) {
                if (ParseUtil.isErrorTail(offset, stmt)) {
                    return (offset << 8) | KILL_CONN;
                }
            }
        }
        return OTHER;
    }

    // KILL @@XA_SESSION XXXXXX
    private static int killXASession(String stmt, int offset) {
        if (stmt.length() > offset + "A_SESSION ".length()) {
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
            if ((c1 == 'A' || c1 == 'a') &&
                    (c2 == '_') &&
                    (c3 == 'S' || c3 == 's') &&
                    (c4 == 'E' || c4 == 'e') &&
                    (c5 == 'S' || c5 == 's') &&
                    (c6 == 'S' || c6 == 's') &&
                    (c7 == 'I' || c7 == 'i') &&
                    (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'N' || c9 == 'n') &&
                    (c10 == ' ' || c10 == '\t' || c10 == '\r' || c10 == '\n')) {
                if (ParseUtil.isErrorTail(offset, stmt)) {
                    return (offset << 8) | KILL_XA_SESSION;
                }
            }
        }
        return OTHER;
    }


    // UPDATE' ' | USE' '
    private static int uCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'P':
                case 'p':
                    if (stmt.length() > offset + 5) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        char c3 = stmt.charAt(++offset);
                        char c4 = stmt.charAt(++offset);
                        char c5 = stmt.charAt(++offset);
                        if ((c1 == 'D' || c1 == 'd') &&
                                (c2 == 'A' || c2 == 'a') &&
                                (c3 == 'T' || c3 == 't') &&
                                (c4 == 'E' || c4 == 'e') &&
                                (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                            return UPDATE;
                        }
                    }
                    break;
                case 'S':
                case 's':
                    if (stmt.length() > offset + 2) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        if ((c1 == 'E' || c1 == 'e') &&
                                (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                            return (offset << 8) | USE;
                        }
                    }
                    break;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

}
