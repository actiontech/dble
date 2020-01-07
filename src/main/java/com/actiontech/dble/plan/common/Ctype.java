/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common;

public final class Ctype {
    private Ctype() {
    }

    private static final int MY_U = 1; /* Upper case */
    private static final int MY_L = 2; /* Lower case */
    private static final int MY_NMR = 4; /* Numeral (digit) */
    private static final int MY_SPC = 8; /* Spacing character */
    private static final int MY_PNT = 16; /* Punctuation */
    public static final int MY_CTR = 32; /* Control character */
    public static final int MY_B = 64; /* Blank */
    public static final int MY_X = 128; /* heXadecimal digit */

    private static byte[] ctypeLatin1 = {0, 32, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 32, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
            16, 16, 16, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132,
            (byte) 132, (byte) 132, 16, 16, 16, 16, 16, 16, 16, (byte) 129, (byte) 129, (byte) 129, (byte) 129,
            (byte) 129, (byte) 129, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 16, 16, 16, 16, 16,
            (byte) 130, (byte) 130, (byte) 130, (byte) 130, (byte) 130, (byte) 130, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2, 16, 16, 16, 16, 32, 16, 0, 16, 2, 16, 16, 16, 16, 16, 16, 1, 16, 1, 0, 1, 0, 0, 16,
            16, 16, 16, 16, 16, 16, 16, 16, 2, 16, 2, 0, 2, 1, 72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
            16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 16, 2, 2, 2, 2, 2, 2, 2, 2};


    public static boolean isDigit(char c) {
        int index = (int) c + 1;
        return (ctypeLatin1[index] & MY_NMR) != 0;
    }

    public static boolean myIsAlpha(char c) {
        int index = (int) c + 1;
        return (ctypeLatin1[index] & (MY_U | MY_L)) != 0;
    }

    public static boolean spaceChar(char c) {
        int index = (int) c + 1;
        return (ctypeLatin1[index] & MY_SPC) != 0;
    }

    public static boolean isPunct(char c) {
        int index = (int) c + 1;
        return (ctypeLatin1[index] & MY_PNT) != 0;
    }


    public static int myStrnncoll(char[] css, int sbegin, int slen, char[] cst, int tbegin, int tlen) {
        return new String(css, sbegin, slen).compareTo(new String(cst, tbegin, tlen));
    }

}
