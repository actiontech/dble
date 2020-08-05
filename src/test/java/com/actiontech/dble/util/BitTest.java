/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import org.junit.Test;

/**
 * @author mycat
 */
public class BitTest {
    @Test
    public void testNoop() {
    }

    public static void main(String[] args) {
        System.out.println(0xffff0001 & 0xffff);// low 16 bits
        System.out.println(0x0002ffff >>> 16);// high 16 bits
    }
}