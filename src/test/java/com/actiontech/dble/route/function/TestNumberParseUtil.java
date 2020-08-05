/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import org.junit.Assert;
import org.junit.Test;

public class TestNumberParseUtil {

    @Test
    public void test() {
        String val = "2000";
        Assert.assertEquals(2000, NumberParseUtil.parseLong(val));
        val = "2M";
        Assert.assertEquals(20000, NumberParseUtil.parseLong(val));
        val = "2M1";
        Assert.assertEquals(20001, NumberParseUtil.parseLong(val));
        val = "1000M";
        Assert.assertEquals(10000000, NumberParseUtil.parseLong(val));
        val = "30K";
        Assert.assertEquals(30000, NumberParseUtil.parseLong(val));
        val = "30K1";
        Assert.assertEquals(30001, NumberParseUtil.parseLong(val));
        val = "30K09";
        Assert.assertEquals(30009, NumberParseUtil.parseLong(val));
    }
}