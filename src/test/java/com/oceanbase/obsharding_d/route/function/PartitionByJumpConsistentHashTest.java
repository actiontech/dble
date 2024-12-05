/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.function;


import org.junit.Assert;
import org.junit.Test;


public class PartitionByJumpConsistentHashTest {

    @Test
    public void test1() {
        PartitionByJumpConsistentHash partition = new PartitionByJumpConsistentHash();
        partition.setHashSlice("0:0");
        partition.setPartitionCount(5);
        partition.init();
        Assert.assertEquals(1, (int) partition.calculate("8"));
        Assert.assertEquals(4, (int) partition.calculate("5"));
    }

    @Test
    public void test2() {
        PartitionByJumpConsistentHash partition = new PartitionByJumpConsistentHash();
        partition.setHashSlice("0:0");
        partition.setPartitionCount(4);
        partition.init();
        Assert.assertEquals(1, (int) partition.calculate("8"));
        Assert.assertEquals(0, (int) partition.calculate("5"));
    }

}