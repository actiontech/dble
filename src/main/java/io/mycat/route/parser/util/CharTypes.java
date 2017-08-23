/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.parser.util;

/**
 * @author mycat
 * @author mycat
 */
public class CharTypes {
    private final static boolean[] HEX_FLAGS = new boolean[256];

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

    private final static boolean[] IDENTIFIER_FLAGS = new boolean[256];

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

    private final static boolean[] WHITESPACE_FLAGS = new boolean[256];

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
