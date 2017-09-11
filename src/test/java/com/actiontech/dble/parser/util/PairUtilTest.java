/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.parser.util;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.PairUtil;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat
 */
public class PairUtilTest extends TestCase {

    @Test
    public void testSequenceSlicing() {
        Assert.assertEquals(new Pair<Integer, Integer>(0, 2), PairUtil.sequenceSlicing("2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 2), PairUtil.sequenceSlicing("1: 2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 0), PairUtil.sequenceSlicing(" 1 :"));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), PairUtil.sequenceSlicing("-1: "));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), PairUtil.sequenceSlicing(" -1:0"));
        Assert.assertEquals(new Pair<Integer, Integer>(0, 0), PairUtil.sequenceSlicing(" :"));
    }

    @Test
    public void testsplitIndexTest() {
        String src1 = "offer_group[10]";
        Pair<String, Integer> pair1 = PairUtil.splitIndex(src1, '[', ']');
        Assert.assertEquals("offer_group", pair1.getKey());
        Assert.assertEquals(Integer.valueOf(10), pair1.getValue());

        String src2 = "offer_group";
        Pair<String, Integer> pair2 = PairUtil.splitIndex(src2, '[', ']');
        Assert.assertEquals("offer_group", pair2.getKey());
        Assert.assertEquals(Integer.valueOf(-1), pair2.getValue());
    }

}
