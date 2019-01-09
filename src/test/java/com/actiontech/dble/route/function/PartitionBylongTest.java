/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartitionBylongTest {
    private Integer[] allNode = new Integer[0];
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCalculate1() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "511";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "512";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "1023";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "1024";
        Assert.assertEquals(true, 0 == rule.calculate(value));
    }

    @Test
    public void testCalculate2() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("1");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "1";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "2";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "-1";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "-2";
        Assert.assertEquals(true, 0 == rule.calculate(value));
    }

    @Test
    public void testCalculate3() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("3");
        rule.setPartitionLength("1");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "1";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "2";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "3";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "-1";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "-2";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "-3";
        Assert.assertEquals(true, 0 == rule.calculate(value));
    }

    @Test
    public void testCalculate4() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("1,2");
        rule.setPartitionLength("1,2");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "1";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "2";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "3";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "4";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "5";
        Assert.assertEquals(true, 0 == rule.calculate(value));
    }

    @Test
    public void testCalculate5() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("3");
        rule.setPartitionLength("1440");
        thrown.expect(RuntimeException.class);
        rule.init();
    }

    @Test
    public void testCalculate6() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("3");
        rule.setPartitionLength("1");
        rule.init();
        String value = "";
        thrown.expect(IllegalArgumentException.class);
        rule.calculate(value);
    }

    @Test
    public void testCalculate7() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("-2");
        rule.setPartitionLength("1");
        thrown.expect(RuntimeException.class);
        rule.init();

    }

    @Test
    public void testCalculate8() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("0");
        rule.setPartitionLength("1");
        thrown.expect(RuntimeException.class);
        rule.init();

    }

    @Test
    public void testCalculate9() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("1,-2");
        rule.setPartitionLength("1,4");
        thrown.expect(RuntimeException.class);
        rule.init();

    }

    @Test
    public void testCalculateRange() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "0";
        String endValue = "512";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{0, 1};
        checkCalculateRange(expect, fact);

    }

    @Test
    public void testCalculateRange2() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "0";
        String endValue = "1024";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange3() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "1";
        String endValue = "1024";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{0, 1};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange4() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "1";
        String endValue = "2048";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange5() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "514";
        String endValue = "1024";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{1, 0};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange6() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("4");
        rule.setPartitionLength("256");
        rule.init();
        String bgeinValue = "769";
        String endValue = "1274";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{3, 0};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange7() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("128");
        rule.init();
        String bgeinValue = "769";
        String endValue = "1274";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{6, 7, 0, 1};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange8() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("128");
        rule.init();
        String bgeinValue = "770";
        String endValue = "1793";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{6, 7, 0, 1, 2, 3, 4, 5};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange9() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("128");
        rule.init();
        String bgeinValue = "770";
        String endValue = "771";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{6};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange10() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("128");
        rule.init();
        String bgeinValue = "769";
        String endValue = "895";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{6};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange11() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("128");
        rule.init();
        String bgeinValue = "769";
        String endValue = "1793";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        checkCalculateRange(allNode, fact);
    }

    @Test
    public void testCalculateRange12() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "-2";
        String endValue = "-1";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{1};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange13() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "-1026";
        String endValue = "-3";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{1, 0};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange14() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "1021";
        String endValue = "1023";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{1};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculateRange15() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("2");
        rule.setPartitionLength("512");
        rule.init();
        String bgeinValue = "1021";
        String endValue = "2043";
        Integer[] fact = rule.calculateRange(bgeinValue, endValue);
        Integer[] expect = new Integer[]{1, 0};
        checkCalculateRange(expect, fact);
    }

    @Test
    public void testCalculate16() {
        PartitionByLong rule = new PartitionByLong();
        rule.setPartitionCount("8");
        rule.setPartitionLength("360");
        rule.init();
        String value = "0";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "359";
        Assert.assertEquals(true, 0 == rule.calculate(value));
        value = "360";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "719";
        Assert.assertEquals(true, 1 == rule.calculate(value));
        value = "720";
        Assert.assertEquals(true, 2 == rule.calculate(value));
        value = "2879";
        Assert.assertEquals(true, 7 == rule.calculate(value));
        value = "2880";
        Assert.assertEquals(true, 0 == rule.calculate(value));
    }

    private void checkCalculateRange(Integer[] expect, Integer[] fact) {
        Assert.assertEquals(true, expect.length == fact.length);
        for (int i = 0; i < expect.length; i++) {
            Assert.assertEquals(true, expect[i].intValue() == fact[i].intValue());
        }
    }
}
