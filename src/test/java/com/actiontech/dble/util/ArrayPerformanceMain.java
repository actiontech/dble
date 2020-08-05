/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public class ArrayPerformanceMain {

    public void tArray() {
        byte[] a = new byte[]{1, 2, 3, 4, 5, 6, 7};
        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int x = 0; x < 1000000; x++) {
            byte[][] ab = new byte[10][];
            for (int i = 0; i < ab.length; i++) {
                ab[i] = a;
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("array take time:" + (t2 - t1) + " ms.");
    }

    public void tList() {
        byte[] a = new byte[]{1, 2, 3, 4, 5, 6, 7};
        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int x = 0; x < 1000000; x++) {
            List<byte[]> ab = new ArrayList<byte[]>(10);
            for (int i = 0; i < ab.size(); i++) {
                ab.add(a);
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("list take time:" + (t2 - t1) + " ms.");
    }

}