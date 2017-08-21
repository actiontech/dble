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
package io.mycat.route.function;


import org.junit.Assert;
import org.junit.Test;

public class AutoPartitionByLongTest {

    @Test
    public void test1() {
        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
        autoPartition.setMapFile("autopartition-long.txt");
        autoPartition.init();
        String idVal = "0";
        Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));

        idVal = "2000000";
        Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));

        idVal = "2000001";
        Assert.assertEquals(true, 1 == autoPartition.calculate(idVal));

        idVal = "4000000";
        Assert.assertEquals(true, 1 == autoPartition.calculate(idVal));

        idVal = "4000001";
        Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));
        idVal = "6000000";
        Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));
        idVal = "6000001";
        Assert.assertEquals(true, null == autoPartition.calculate(idVal));
    }

    @Test
    public void test2() {
        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
        autoPartition.setMapFile("autopartition-long.txt");
        autoPartition.setDefaultNode(0);
        autoPartition.init();
        String idVal = "6000001";
        Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));
    }

    @Test
    public void test3() {
        AutoPartitionByLong autoPartition = new AutoPartitionByLong();
        autoPartition.setMapFile("autopartition-long.txt");
        autoPartition.setDefaultNode(0);
        autoPartition.init();

        Integer[] res = autoPartition.calculateRange("-1", "9999999999");
        Assert.assertEquals(3, res.length);

        res = autoPartition.calculateRange("-1", "10000000");
        Assert.assertEquals(3, res.length);

        res = autoPartition.calculateRange("-1", "100");
        Assert.assertEquals(3, res.length);

        res = autoPartition.calculateRange("0", "100");
        Assert.assertEquals(1, res.length);

        res = autoPartition.calculateRange("0", "100000");
        Assert.assertEquals(1, res.length);


        res = autoPartition.calculateRange("2000009", "3999999");
        Assert.assertEquals(1, res.length);

        res = autoPartition.calculateRange("2000009", "5999999");
        Assert.assertEquals(2, res.length);


        res = autoPartition.calculateRange("2000009", "59999999");
        Assert.assertEquals(3, res.length);

    }


}