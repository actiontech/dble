/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartitionByDateTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test1() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsPartionDay("10");

        partition.init();

        Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-01-11"));
        Assert.assertEquals(true, 12 == partition.calculate("2014-05-01"));


    }

    @Test
    public void test2() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-31");
        partition.setsPartionDay("10");
        partition.init();

        /**
         * 0 : 01.01-01.10,02.10-02.19
         * 1 : 01.11-01.20,02.20-03.01
         * 2 : 01.21-01.30,03.02-03.12
         * 3  : 01.31-02-09,03.13-03.23
         */
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-01-11"));
        Assert.assertEquals(true, 3 == partition.calculate("2014-01-31"));
        Assert.assertEquals(true, 3 == partition.calculate("2014-02-01"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-02-19"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-02-20"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-03-01"));
        Assert.assertEquals(true, 2 == partition.calculate("2014-03-02"));
        Assert.assertEquals(true, 2 == partition.calculate("2014-03-11"));
        Assert.assertEquals(true, 3 == partition.calculate("2014-03-20"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-03-24"));


    }

    @Test
    public void test3() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-30");
        partition.setsPartionDay("10");
        partition.init();

        /**
         * 0 : 01.01-01.10,01.31-02.09
         * 1 : 01.11-01.20,02.10-02.19
         * 2 : 01.21-01.30,02.20-03.01
         */
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-01"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-10"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-01-11"));
        Assert.assertEquals(true, 2 == partition.calculate("2014-01-30"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-31"));
        Assert.assertEquals(true, 0 == partition.calculate("2014-02-09"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-02-10"));
        Assert.assertEquals(true, 1 == partition.calculate("2014-02-19"));
        Assert.assertEquals(true, 2 == partition.calculate("2014-02-20"));
        Assert.assertEquals(true, 2 == partition.calculate("2014-03-01"));


    }

    @Test
    public void test4() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-30");
        partition.setsPartionDay("10");
        partition.init();
        thrown.expect(IllegalArgumentException.class);
        partition.calculate("2014/01/01");
    }

    @Test
    public void test5() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-30");
        partition.setsPartionDay("10");
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculate("2014-01-01 12:00:03"));
    }

    @Test
    public void test6() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-30");
        partition.setsPartionDay("10");
        partition.init();
        Assert.assertEquals(true, null == partition.calculate("2012-12-31"));
    }

    @Test
    public void test7() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsEndDate("2014-01-30");
        partition.setsPartionDay("10");
        partition.setDefaultNode(0);
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculate("2012-12-31"));
    }

    @Test
    public void test8() {
        PartitionByDate partition = new PartitionByDate();

        partition.setDateFormat("yyyy-MM-dd");
        partition.setsBeginDate("2014-01-01");
        partition.setsPartionDay("10");
        partition.setDefaultNode(0);
        partition.init();
        Assert.assertEquals(true, 0 == partition.calculate("2012-12-31"));
    }
}