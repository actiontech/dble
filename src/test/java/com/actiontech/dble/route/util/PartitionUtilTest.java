/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat()
 */
public class PartitionUtilTest {

    @Test
    public void testPartition() {
        // eg:
        // |<---------------------1024------------------------>|
        // |<----256--->|<----256--->|<----------512---------->|
        // | partition0 | partition1 | partition2 |
        // | count[0]=2 | count[1]=1 |
        int[] count = new int[]{2, 1};
        int[] length = new int[]{256, 512};
        PartitionUtil pu = new PartitionUtil(count, length);

        int DEFAULT_STR_HEAD_LEN = 8;
        long offerId = 12345;
        String memberId = "qiushuo";

        // expected partition0
        int partNo1 = pu.partition(offerId);

        // expected partition2
        int partNo2 = pu.partition(memberId, 0, DEFAULT_STR_HEAD_LEN);

        Assert.assertEquals(0, partNo1);
        Assert.assertEquals(2, partNo2);
    }

}