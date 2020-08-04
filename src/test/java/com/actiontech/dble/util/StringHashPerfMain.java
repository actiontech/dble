/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

/**
 * @author mycat
 */
public class StringHashPerfMain {

    public static void main(String[] args) {
        String s = "abcdejdsalfp";
        int end = s.length();
        for (int i = 0; i < 10; i++) {
            StringUtil.hash(s, 0, end);
        }
        long loop = 10000 * 10000;
        long t1 = System.currentTimeMillis();
        t1 = System.currentTimeMillis();
        for (long i = 0; i < loop; ++i) {
            StringUtil.hash(s, 0, end);
        }
        long t2 = System.currentTimeMillis();
        System.out.println((((t2 - t1) * 1000 * 1000) / loop) + " ns.");
    }

}