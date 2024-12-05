package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

import static com.oceanbase.obsharding_d.server.parser.ServerParse.*;

public class ServerParseValidations {
    public ServerParseValidations() {
    }

    public int flushCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'U' || c2 == 'u') &&
                    (c3 == 'S' || c3 == 's') && (c4 == 'H' || c4 == 'h') &&
                    ParseUtil.isSpace(stmt.charAt(++offset))) {
                return FLUSH;
            }
        }
        return OTHER;
    }

    public int loadCheck(String stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                return loadParse(stmt, offset);
            }
        }
        return OTHER;
    }

    private int loadParse(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't') && (c4 == 'A' || c4 == 'a') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return LOAD_DATA_INFILE_SQL;
            } else if ((c1 == 'I' || c1 == 'i') && (c2 == 'N' || c2 == 'n') && (c3 == 'D' || c3 == 'd') && (c4 == 'E' || c4 == 'e') &&
                    (c5 == 'X' || c5 == 'x')) {
                if (stmt.length() > offset + 1) {
                    char c6 = stmt.charAt(++offset);
                    if ((c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                        return UNSUPPORT;
                    }
                }
            }
        }
        return OTHER;
    }

    public int lockCheck(String stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'K' || c1 == 'k') && (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                return LOCK;
            }
        }
        return OTHER;
    }

    public int migrateCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);

            if ((c1 == 'i' || c1 == 'I') &&
                    (c2 == 'g' || c2 == 'G') &&
                    (c3 == 'r' || c3 == 'R') &&
                    (c4 == 'a' || c4 == 'A') &&
                    (c5 == 't' || c5 == 'T') &&
                    (c6 == 'e' || c6 == 'E')) {
                return MIGRATE;
            }
        }
        return OTHER;
    }

    public int optimizeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'P' || c1 == 'p') && (c2 == 'T' || c2 == 't') && (c3 == 'I' || c3 == 'i') && (c4 == 'M' || c4 == 'm') &&
                    (c5 == 'I' || c5 == 'i') && (c6 == 'Z' || c6 == 'z') && (c7 == 'E' || c7 == 'e') &&
                    (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return UNSUPPORT;
            }
        }
        return OTHER;
    }

    public boolean isPrepare(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'E' || c2 == 'e') && (c3 == 'P' || c3 == 'p') && (c4 == 'A' || c4 == 'a') &&
                    (c5 == 'R' || c5 == 'r') && (c6 == 'E' || c6 == 'e') &&
                    (c7 == ' ' || c7 == '\t' || c7 == '\r' || c7 == '\n')) {
                return true;
            }
        }
        return false;
    }

    //truncate
    public int tCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);

            if ((c1 == 'R' || c1 == 'r') && (c2 == 'U' || c2 == 'u') && (c3 == 'N' || c3 == 'n') && (c4 == 'C' || c4 == 'c') &&
                    (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't') && (c7 == 'E' || c7 == 'e') &&
                    (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return DDL;
            }
        }
        return OTHER;
    }

    public int alterCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'T' || c1 == 't') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')) {
                return alterViewCheck(stmt, offset);
            }
        }
        return OTHER;
    }

    private int alterViewCheck(String stmt, int offset) {
        while (true) {
            if (!(stmt.charAt(++offset) == ' ' || stmt.charAt(offset) == '\t' || stmt.charAt(offset) == '\r' || stmt.charAt(offset) == '\n')) {
                char c1 = stmt.charAt(offset);
                char c2 = stmt.charAt(++offset);
                char c3 = stmt.charAt(++offset);
                char c4 = stmt.charAt(++offset);
                char c5 = stmt.charAt(++offset);
                if ((c1 == 'v' || c1 == 'V') && (c2 == 'i' || c2 == 'I') && (c3 == 'e' || c3 == 'E') && (c4 == 'w' || c4 == 'W') &&
                        (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                    return ALTER_VIEW;
                } else {
                    return DDL;
                }
            }
        }
    }

    public int analyzeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') && (c3 == 'Y' || c3 == 'y') &&
                    (c4 == 'Z' || c4 == 'z') && (c5 == 'E' || c5 == 'e') && (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                return UNSUPPORT;
            }
        }
        return OTHER;
    }

    public int databaseCheck(String stmt, int offset) {
        int len = stmt.length();
        if (len > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'a' || c1 == 'A') && (c2 == 't' || c2 == 'T') && (c3 == 'A' || c3 == 'a') &&
                    (c4 == 'b' || c4 == 'B') && (c5 == 'a' || c5 == 'A') && (c6 == 's' || c6 == 'S') &&
                    (c7 == 'e' || c7 == 'E') && ParseUtil.isSpace(stmt.charAt(++offset))) {
                return CREATE_DATABASE;
            }
        }
        return DDL;
    }

    public int viewCheck(String stmt, int offset, boolean isReplace) {
        int len = stmt.length();
        if (len > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'W' || c3 == 'w') && ParseUtil.isSpace(stmt.charAt(++offset))) {
                if (isReplace) {
                    return REPLACE_VIEW;
                } else {
                    return CREATE_VIEW;
                }
            }
        }
        return DDL;
    }

    // HELP' '
    public int helpCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ELP ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'L' || c2 == 'l') &&
                    (c3 == 'P' || c3 == 'p')) {
                return (offset << 8) | HELP;
            }
        }
        return OTHER;
    }

    //EXECUTE' '
    public int executeCheck(String stmt, int offset) {
        if (stmt.length() > offset + "CUTE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'C' || c1 == 'c') && (c2 == 'U' || c2 == 'u') && (c3 == 'T' || c3 == 't') && (c4 == 'E' || c4 == 'e') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return SCRIPT_PREPARE;
            }
        }
        return OTHER;
    }

    // EXPLAIN' '
    public int explainCheck(String stmt, int offset) {
        if (stmt.length() > offset + "LAIN ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'A' || c2 == 'a') && (c3 == 'I' || c3 == 'i') && (c4 == 'N' || c4 == 'n')) {
                if (ParseUtil.isSpaceOrLeft(c5)) {
                    return (offset << 8) | EXPLAIN;
                } else if (c5 == '2' && (stmt.length() > offset + 1) && ParseUtil.isSpace(stmt.charAt(++offset))) {
                    return (offset << 8) | EXPLAIN2;
                } else {
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // KILL QUERY' '
    public int killQueryCheck(String stmt, int offset) {
        if (stmt.length() > offset + "UERY ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'R' || c3 == 'r') && (c4 == 'Y' || c4 == 'y') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            continue;
                        default:
                            return (offset << 8) | KILL_QUERY;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // BEGIN
    public int beginCheck(String stmt, int offset) {
        String key = "work";
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') &&
                    (c2 == 'G' || c2 == 'g') &&
                    (c3 == 'I' || c3 == 'i') &&
                    (c4 == 'N' || c4 == 'n') &&
                    (stmt.length() == ++offset || keyCheck(stmt, key, offset) || ParseUtil.isEOF(stmt, offset) || ParseUtil.isMultiEof(stmt, offset))) {
                return BEGIN;
            }
        }
        return OTHER;
    }

    // COMMIT
    public int commitCheck(String stmt, int offset) {
        String key = "work";
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') &&
                    (c2 == 'M' || c2 == 'm') &&
                    (c3 == 'M' || c3 == 'm') &&
                    (c4 == 'I' || c4 == 'i') &&
                    (c5 == 'T' || c5 == 't') &&
                    (stmt.length() == ++offset || keyCheck(stmt, key, offset) || ParseUtil.isEOF(stmt, offset) || ParseUtil.isMultiEof(stmt, offset))) {
                return COMMIT;
            }
        }

        return OTHER;
    }

    private boolean keyCheck(String stmt, String key, int offset) {
        String lowerStmt = stmt.toLowerCase().substring(offset).trim();
        offset = stmt.toLowerCase().indexOf(key) + key.length() - 1;
        if (lowerStmt.startsWith(key) && (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset) || ParseUtil.isMultiEof(stmt, offset))) {
            return true;
        }
        return false;
    }

    // CALL
    public int callCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') &&
                    (c3 == 'L' || c3 == 'l')) {
                return CALL;
            }
        }

        return OTHER;
    }

    public int checksumCheck(String stmt, int offset) {
        if (stmt.length() > offset + "HECKSUM ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'E' || c2 == 'e') && (c3 == 'C' || c3 == 'c') && (c4 == 'K' || c4 == 'k') &&
                    (c5 == 'S' || c5 == 's') && (c6 == 'U' || c6 == 'u') && (c7 == 'M' || c7 == 'm') &&
                    (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return UNSUPPORT;
            }
        }
        return OTHER;
    }

    public int dealCheck(String stmt, int offset) {
        if (stmt.length() > offset + "LLOCATE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'L' || c2 == 'l') && (c3 == 'O' || c3 == 'o') && (c4 == 'C' || c4 == 'c') &&
                    (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't') && (c7 == 'E' || c7 == 'e') &&
                    (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return SCRIPT_PREPARE;
            }
        }
        return OTHER;
    }

    public int descCheck(String stmt, int offset) {
        if (stmt.length() > offset + "C ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if (c1 == 'C' || c1 == 'c') {
                if (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n') {
                    return DESCRIBE;
                } else if (c2 == 'R' || c2 == 'r') {
                    if (stmt.length() > offset + "IBE ".length()) {
                        char c3 = stmt.charAt(++offset);
                        char c4 = stmt.charAt(++offset);
                        char c5 = stmt.charAt(++offset);
                        char c6 = stmt.charAt(++offset);
                        if ((c3 == 'I' || c3 == 'i') && (c4 == 'B' || c4 == 'b') && (c5 == 'E' || c5 == 'e') &&
                                (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                            return DESCRIBE;
                        }
                    }
                }
            }
        }
        return OTHER;
    }

    public int deleCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ETE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'T' || c2 == 't') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return DELETE;
            }
        }
        return OTHER;
    }

    // INSERT' '
    public int insertCheck(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's') &&
                    (c3 == 'E' || c3 == 'e') && (c4 == 'R' || c4 == 'r') &&
                    (c5 == 'T' || c5 == 't') &&
                    (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                return INSERT;
            }
        }
        return OTHER;
    }

    public int release(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'e' || c1 == 'E') && (c2 == 'a' || c2 == 'A') && (c3 == 's' || c3 == 'S') &&
                    (c4 == 'e' || c4 == 'E') && ParseUtil.isSpace(c5)) {
                return RELEASE_SAVEPOINT;
            }
        }
        return OTHER;
    }

    public int repair(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'R' || c2 == 'r') && (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
                return UNSUPPORT;
            }
        }
        return OTHER;
    }

    public int replace(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'C' || c2 == 'c') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return REPLACE;
            }
        }
        return OTHER;
    }

    public int rollbackWorkCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'K' || c3 == 'k')) {
                if (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset) || ParseUtil.isMultiEof(stmt, offset)) {
                    return ROLLBACK;
                }
            }
        }
        return OTHER;
    }

    public int savepointCheck(String stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'V' || c1 == 'v') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'P' || c3 == 'p') && (c4 == 'O' || c4 == 'o') &&
                    (c5 == 'I' || c5 == 'i') && (c6 == 'N' || c6 == 'n') &&
                    (c7 == 'T' || c7 == 't') &&
                    (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return SAVEPOINT;
            }
        }
        return OTHER;
    }

    public int selectCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') &&
                    (c2 == 'C' || c2 == 'c') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n' || c4 == '/' || c4 == '#')) {
                return (offset << 8) | SELECT;
            }
        }
        return OTHER;
    }

    public int showCheck(String stmt, int offset) {
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

    // KILL CONNECTION' 'XXXXXX
    public int killConnection(String stmt, int offset) {
        final String keyword = "CONNECTION";
        if (ParseUtil.compare(stmt, offset, keyword)) {
            offset = offset + keyword.length();
            if (stmt.length() > offset && ParseUtil.isSpace(stmt.charAt(offset)) && ParseUtil.isErrorTail(offset + 1, stmt)) {
                return (offset << 8) | KILL;
            }
        }
        return OTHER;
    }

    //revoke
    public int revoke(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'o' || c1 == 'O') && (c2 == 'k' || c2 == 'K') && (c3 == 'e' || c3 == 'E') && ParseUtil.isSpace(c4)) {
                return REVOKE;
            }
        }
        return OTHER;
    }

    public int xaCheck(String stmt, int offset) {
        char c1 = stmt.charAt(++offset);
        if (((c1 == 'S' || c1 == 's') || (c1 == 'B' || c1 == 'b')) &&
                stmt.length() > offset + 5 &&
                (stmt.substring(offset + 1, offset + 5).equalsIgnoreCase("TART") ||
                        stmt.substring(offset + 1, offset + 5).equalsIgnoreCase("EGIN"))) {
            offset += 5;
            return (offset << 8) | XA_START;
        } else if ((c1 == 'E' || c1 == 'e') &&
                stmt.length() > offset + 3 &&
                stmt.substring(offset + 1, offset + 3).equalsIgnoreCase("ND")) {
            offset += 3;
            return (offset << 8) | XA_END;
        } else if ((c1 == 'P' || c1 == 'p') &&
                stmt.length() > offset + 7 &&
                stmt.substring(offset + 1, offset + 7).equalsIgnoreCase("REPARE")) {
            offset += 7;
            return (offset << 8) | XA_PREPARE;
        } else if ((c1 == 'R' || c1 == 'r') &&
                stmt.length() > offset + 8 &&
                stmt.substring(offset + 1, offset + 8).equalsIgnoreCase("OLLBACK")) {
            offset += 8;
            return (offset << 8) | XA_ROLLBACK;
        } else if ((c1 == 'C' || c1 == 'c') &&
                stmt.length() > offset + 6 &&
                stmt.substring(offset + 1, offset + 6).equalsIgnoreCase("OMMIT")) {
            offset += 6;
            return (offset << 8) | XA_COMMIT;
        } else {
            return OTHER;
        }
    }

    //create TEMPORARY TABLE XXXX
    public int createTempTableCheck(String stmt, int offset) {
        String keyword = "EMPORARY";
        if (!ParseUtil.compare(stmt, offset, keyword)) {
            return DDL;
        }
        offset += keyword.length();
        offset = ParseUtil.skipSpace(stmt, offset);
        keyword = "TABLE";
        if (!ParseUtil.compare(stmt, offset, keyword)) {
            return DDL;
        }
        offset += keyword.length();
        offset = ParseUtil.skipSpace(stmt, offset);
        return (offset << 8) | CREATE_TEMPORARY_TABLE;
    }

    //DROP [TEMPORARY] TABLE [IF EXISTS]
    // tbl_name [, tbl_name] ...
    // [RESTRICT | CASCADE]
    public int dropTableCheck(String stmt, int offset) {
        String keyword = "TEMPORARY";
        if (ParseUtil.compare(stmt, offset, keyword)) {
            offset += keyword.length();
            offset = ParseUtil.skipSpace(stmt, offset);
        }
        keyword = "TABLE";
        if (!ParseUtil.compare(stmt, offset, keyword)) {
            return DDL;
        }
        offset += keyword.length();
        offset = ParseUtil.skipSpace(stmt, offset);
        return (offset << 8) | DROP_TABLE;
    }

    public int instCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') && (c3 == 'L' || c3 == 'l') && ParseUtil.isSpace(c4)) {
                return INSTALL;
            }
        }
        return OTHER;
    }

    public int inseCheck(String stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') && ParseUtil.isSpace(c3)) {
                return INSERT;
            }
        }
        return OTHER;
    }

    //grant
    public int gCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') &&
                    (c2 == 'A' || c2 == 'a') &&
                    (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'T' || c4 == 't') && ParseUtil.isSpace(c5)) {
                return GRANT;
            }
        }
        return OTHER;
    }

    public int uniCheck(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') &&
                    (c2 == 'S' || c2 == 's') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') &&
                    (c5 == 'L' || c5 == 'l') &&
                    (c6 == 'L' || c6 == 'l') &&
                    ParseUtil.isSpace(c7)) {
                return UNINSTALL;
            }
        }
        return OTHER;
    }

    public int unlCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') &&
                    (c2 == 'C' || c2 == 'c') &&
                    (c3 == 'K' || c3 == 'k') &&
                    ParseUtil.isSpace(c4)) {
                return UNLOCK;
            }
        }
        return OTHER;
    }
}
