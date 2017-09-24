/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

/**
 * @author mycat
 * @author mycat
 */
public final class CharTypes {
    private CharTypes() {
    }

    private static final boolean[] HEX_FLAGS = new boolean[256];

    static {
        for (char c = 0; c < HEX_FLAGS.length; ++c) {
            if (c >= 'A' && c <= 'F') {
                HEX_FLAGS[c] = true;
            } else if (c >= 'a' && c <= 'f') {
                HEX_FLAGS[c] = true;
            } else if (c >= '0' && c <= '9') {
                HEX_FLAGS[c] = true;
            }
        }
    }

    public static boolean isHex(char c) {
        return c < 256 && HEX_FLAGS[c];
    }

    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static final boolean[] IDENTIFIER_FLAGS = new boolean[256];

    static {
        for (char c = 0; c < IDENTIFIER_FLAGS.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                IDENTIFIER_FLAGS[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                IDENTIFIER_FLAGS[c] = true;
            } else if (c >= '0' && c <= '9') {
                IDENTIFIER_FLAGS[c] = true;
            }
        }
        //  IDENTIFIER_FLAGS['`'] = true;
        IDENTIFIER_FLAGS['_'] = true;
        IDENTIFIER_FLAGS['$'] = true;
    }

    public static boolean isIdentifierChar(char c) {
        return c > IDENTIFIER_FLAGS.length || IDENTIFIER_FLAGS[c];
    }

    private static final boolean[] WHITESPACE_FLAGS = new boolean[256];

    static {
        WHITESPACE_FLAGS[' '] = true;
        WHITESPACE_FLAGS['\n'] = true;
        WHITESPACE_FLAGS['\r'] = true;
        WHITESPACE_FLAGS['\t'] = true;
        WHITESPACE_FLAGS['\f'] = true;
        WHITESPACE_FLAGS['\b'] = true;
    }

    /**
     * @return false if {@link MySQLLexer#EOI}
     */
    public static boolean isWhitespace(char c) {
        return c <= WHITESPACE_FLAGS.length && WHITESPACE_FLAGS[c];
    }

}
