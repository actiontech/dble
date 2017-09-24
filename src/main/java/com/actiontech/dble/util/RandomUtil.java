/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

/**
 * @author mycat
 */
public final class RandomUtil {
    private RandomUtil() {
    }

    private static final byte[] BYTES = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w', 'e', 'r', 't',
            'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm',
            'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X',
            'C', 'V', 'B', 'N', 'M'};
    private static final long MULTIPLIER = 0x5DEECE66DL;
    private static final long ADDEND = 0xBL;
    private static final long MASK = (1L << 48) - 1;
    private static final long INTEGER_MASK = (1L << 33) - 1;
    private static final long SEED_UNIQUIFIER = 8682522807148012L;

    private static long seed;

    static {
        long s = SEED_UNIQUIFIER + System.nanoTime();
        s = (s ^ MULTIPLIER) & MASK;
        seed = s;
    }

    public static byte[] randomBytes(int size) {
        byte[] bb = BYTES;
        byte[] ab = new byte[size];
        for (int i = 0; i < size; i++) {
            ab[i] = randomByte(bb);
        }
        return ab;
    }

    private static byte randomByte(byte[] b) {
        int ran = (int) ((next() & INTEGER_MASK) >>> 16);
        return b[ran % b.length];
    }

    private static long next() {
        long oldSeed = seed;
        long nextSeed = 0L;
        do {
            nextSeed = (oldSeed * MULTIPLIER + ADDEND) & MASK;
        } while (oldSeed == nextSeed);
        seed = nextSeed;
        return nextSeed;
    }

}
