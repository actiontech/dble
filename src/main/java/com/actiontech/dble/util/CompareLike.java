/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

public class CompareLike {
    private enum CompareType {
        IS_NULL, FULL_MATCH, LIKE
    }

    private static final int MATCH = 0, ONE = 1, ANY = 2;
    private final CompareType type;

    private char[] patternChars;
    private String patternString;
    private int[] patternTypes;
    private int patternLength;

    public CompareLike(String pattern) {
        this(pattern, "");
    }

    public CompareLike(String pattern, String escape) {
        if ("%".equals(pattern)) {
            type = CompareType.IS_NULL;
        } else {
            initPattern(pattern, getEscapeChar(escape));
            if (isFullMatch()) {
                type = CompareType.FULL_MATCH;
            } else {
                type = CompareType.LIKE;
            }
        }
    }

    private Character getEscapeChar(String es) {
        Character esc;
        if (es == null) {
            esc = getEscapeChar("//");
        } else if (es.length() == 0) {
            esc = null;
        } else {
            esc = es.charAt(0);
        }
        return esc;
    }

    private void initPattern(String p, Character escapeChar) {
        patternLength = 0;
        if (p == null) {
            patternTypes = null;
            patternChars = null;
            return;
        }
        int len = p.length();
        patternChars = new char[len];
        patternTypes = new int[len];
        boolean lastAny = false;
        for (int i = 0; i < len; i++) {
            char c = p.charAt(i);
            int patternType;
            if (escapeChar != null && escapeChar == c) {
                if (i >= len - 1) {
                    // invalidPattern = true;
                    return;
                }
                c = p.charAt(++i);
                patternType = MATCH;
                lastAny = false;
            } else if (c == '%') {
                if (lastAny) {
                    continue;
                }
                patternType = ANY;
                lastAny = true;
            } else if (c == '_') {
                patternType = ONE;
            } else {
                patternType = MATCH;
                lastAny = false;
            }
            patternTypes[patternLength] = patternType;
            patternChars[patternLength++] = c;
        }
        for (int i = 0; i < patternLength - 1; i++) {
            if ((patternTypes[i] == ANY) && (patternTypes[i + 1] == ONE)) {
                patternTypes[i] = ONE;
                patternTypes[i + 1] = ANY;
            }
        }
        patternString = new String(patternChars, 0, patternLength);
    }

    private boolean isFullMatch() {
        if (patternTypes == null) {
            return false;
        }
        for (int patternType : patternTypes) {
            if (patternType != MATCH) {
                return false;
            }
        }
        return true;
    }

    public boolean compare(String s) {
        if (type == CompareType.IS_NULL) {
            return s != null;
        } else if (type == CompareType.FULL_MATCH) {
            return StringUtil.equalsIgnoreCase(patternString, s);
        } else {
            return compareAt(s, 0, 0, s.length(), patternChars, patternTypes);
        }
    }

    private boolean compare(char[] pattern, String s, int pi, int si) {
        if (s == null || s.length() == 0) {
            return false;
        }
        return pattern[pi] == s.charAt(si) || equalsChars(patternString, pi, s, si, true);
    }

    private boolean compareAt(String s, int pi, int si, int sLen, char[] pattern, int[] types) {
        for (; pi < patternLength; pi++) {
            switch (types[pi]) {
                case MATCH:
                    if ((si >= sLen) || !compare(pattern, s, pi, si++)) {
                        return false;
                    }
                    break;
                case ONE:
                    if (si++ >= sLen) {
                        return false;
                    }
                    break;
                case ANY:
                    if (++pi >= patternLength) {
                        return true;
                    }
                    while (si < sLen) {
                        if (compare(pattern, s, pi, si) && compareAt(s, pi, si, sLen, pattern, types)) {
                            return true;
                        }
                        si++;
                    }
                    return false;
                default:
                    break;
            }
        }
        return si == sLen;
    }

    public boolean equalsChars(String a, int ai, String b, int bi, boolean ignoreCase) {
        char ca = a.charAt(ai);
        char cb = b.charAt(bi);
        if (ignoreCase) {
            ca = Character.toUpperCase(ca);
            cb = Character.toUpperCase(cb);
        }
        return ca == cb;
    }
}
