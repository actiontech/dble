/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByStringTest {

    @Test
    public void test() {
        PartitionByString rule = new PartitionByString();
        String idVal = null;
        rule.setPartitionLength("512");
        rule.setPartitionCount("2");
        rule.init();
        rule.setHashSlice("0:2");
        //		idVal = "0";
        //		Assert.assertEquals(true, 0 == rule.calculate(idVal));
        //		idVal = "45a";
        //		Assert.assertEquals(true, 1 == rule.calculate(idVal));


        //last 4
        rule = new PartitionByString();
        rule.setPartitionLength("512");
        rule.setPartitionCount("2");
        rule.init();
        //last 4 characters
        rule.setHashSlice("-4:0");
        idVal = "aaaabbb0000";
        Assert.assertEquals(true, 0 == rule.calculate(idVal));
        idVal = "aaaabbb2359";
        Assert.assertEquals(true, 0 == rule.calculate(idVal));
    }
}