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
package io.mycat.util;

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
