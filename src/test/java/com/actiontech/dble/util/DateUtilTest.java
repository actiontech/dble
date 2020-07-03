package com.actiontech.dble.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;


public class DateUtilTest {

    @Test
    public void testDiffDays() {
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        cal1.set(2021, 1, 1);
        cal2.set(2020, 1, 1);
        long diffDays = DateUtil.diffDays(cal1, cal2);
        Assert.assertEquals(366, diffDays);
        cal1.set(2021, 3, 1);
        cal2.set(2020, 3, 1);
        long diffDays2 = DateUtil.diffDays(cal1, cal2);
        Assert.assertEquals(365, diffDays2);
    }

}