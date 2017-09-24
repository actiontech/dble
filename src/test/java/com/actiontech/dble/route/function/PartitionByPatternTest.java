/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import org.junit.Assert;
import org.junit.Test;

public class PartitionByPatternTest {

    @Test
    public void test1() {
        PartitionByPattern autoPartition = new PartitionByPattern();
        autoPartition.setPatternValue(256);
        autoPartition.setDefaultNode(2);
        autoPartition.setMapFile("partition-pattern.txt");
        autoPartition.init();
        String idVal = "0";
        Assert.assertEquals(true, 7 == autoPartition.calculate(idVal));
        idVal = "45a";
        Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));

        Integer[] err1 = autoPartition.calculateRange("45a", "0");
        Assert.assertEquals(true, 8 == err1.length);
        Integer[] err2 = autoPartition.calculateRange("45", "0");
        Assert.assertEquals(true, 0 == err2.length);

        Integer[] normal = autoPartition.calculateRange("0", "45");
        Assert.assertEquals(true, 3 == normal.length);

        Integer[] type1 = autoPartition.calculateRange("1", "45");
        Assert.assertEquals(true, 2 == type1.length);

        Integer[] type2 = autoPartition.calculateRange("200", "260");
        Assert.assertEquals(true, 3 == type2.length);

        Integer[] type3 = autoPartition.calculateRange("200", "456");
        Assert.assertEquals(true, 8 == type3.length);
    }

    /*
        public void test2() {
		PartitionByPattern autoPartition = new PartitionByPattern();
		autoPartition.setPatternValue(256);
		autoPartition.setDefaultNode(2);
		autoPartition.setMapFile("partition-pattern1.txt");
		autoPartition.init();
		String idVal = "0";
		Assert.assertEquals(true, 0 == autoPartition.calculate(idVal));
		idVal = "45a";
		Assert.assertEquals(true, 2 == autoPartition.calculate(idVal));

		Integer [] err1 = autoPartition.calculateRange("45a", "0");
		Assert.assertEquals(true, 9 == err1.length);
		Integer [] err2 = autoPartition.calculateRange("45", "0");
		Assert.assertEquals(true, 0 == err2.length);

		Integer [] type1 = autoPartition.calculateRange("0", "45");
		Assert.assertEquals(true, 3 == type1.length);

		Integer [] type2 = autoPartition.calculateRange("200", "260");
		Assert.assertEquals(true, 4 == type2.length);

		Integer [] type3 = autoPartition.calculateRange("200", "456");
		Assert.assertEquals(true, 9 == type3.length);
	}
    */
    public static void main(String[] args) {
        PartitionByPatternTest test = new PartitionByPatternTest();
        test.test1();
        //test.test2();
        return;
    }
}
