/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.mpp;

import com.actiontech.dble.util.ByteUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestSorter {

    @Test
    public void testDecimal() {
        String d1 = "-1223.000";
        byte[] d1b = d1.getBytes();
        Assert.assertEquals(true, -1223.0 == ByteUtil.getDouble(d1b));
        d1b = "-99999.890".getBytes();
        Assert.assertEquals(true, -99999.890 == ByteUtil.getDouble(d1b));
        // 221346.000
        byte[] data2 = new byte[]{50, 50, 49, 51, 52, 54, 46, 48, 48, 48};
        Assert.assertEquals(true, 221346.000 == ByteUtil.getDouble(data2));
        // 1234567890
        byte[] data3 = new byte[]{49, 50, 51, 52, 53, 54, 55, 56, 57, 48};
        Assert.assertEquals(true, 1234567890 == ByteUtil.getInt(data3));

        // 0123456789
        byte[] data4 = new byte[]{48, 49, 50, 51, 52, 53, 54, 55, 56, 57};
        Assert.assertEquals(true, 123456789 == ByteUtil.getInt(data4));
    }

    @Test
    public void testNumberCompare() {
        byte[] b1 = "0".getBytes();
        byte[] b2 = "0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) == 0);

        b1 = "0".getBytes();
        b2 = "1".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);

        b1 = "10".getBytes();
        b2 = "1".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) > 0);

        b1 = "100.0".getBytes();
        b2 = "100.0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) == 0);

        b1 = "100.000".getBytes();
        b2 = "100.0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) > 0);

        b1 = "-100.000".getBytes();
        b2 = "-100.0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);

        b1 = "-100.001".getBytes();
        b2 = "-100.0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);

        b1 = "-100.001".getBytes();
        b2 = "100.0".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);

        b1 = "90".getBytes();
        b2 = "10000".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);
        b1 = "-90".getBytes();
        b2 = "-10000".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) > 0);

        b1 = "98".getBytes();
        b2 = "98000".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) < 0);

        b1 = "-98".getBytes();
        b2 = "-98000".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) > 0);

        b1 = "12002585786".getBytes();
        b2 = "12002585785".getBytes();
        Assert.assertEquals(true, ByteUtil.compareNumberByte(b1, b2) > 0);

    }
}