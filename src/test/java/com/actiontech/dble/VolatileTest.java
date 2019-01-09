/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;

import org.junit.Test;

/**
 * @author mycat
 */
public class VolatileTest {
    @Test
    public void testNoop() {
    }

    static class VolatileObject {
        volatile Object object = new Object();
    }

    public static void main(String[] args) {
        final VolatileObject vo = new VolatileObject();

        // set
        new Thread() {
            @Override
            public void run() {
                System.out.print("set...");
                while (true) {
                    vo.object = new Object();
                }
            }
        }.start();

        // get
        new Thread() {
            @Override
            public void run() {
                System.out.print("get...");
                while (true) {
                    Object oo = vo.object;
                    oo.toString();
                }
            }
        }.start();
    }

}