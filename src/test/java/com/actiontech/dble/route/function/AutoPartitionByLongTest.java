/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;


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