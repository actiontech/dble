/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

import com.actiontech.dble.cache.impl.EnchachePooFactory;
import junit.framework.Assert;
import org.junit.Test;

public class DefaultLayedCachePoolTest {

    private static DefaultLayedCachePool layedCachePool;

    static {

        layedCachePool = new DefaultLayedCachePool("defaultLayedPool", new EnchachePooFactory(), 1000, 1);

    }

    @Test
    public void testBasic() {
        layedCachePool.putIfAbsent("2", "dn2");
        layedCachePool.putIfAbsent("1", "dn1");

        layedCachePool.putIfAbsent("company", 1, "dn1");
        layedCachePool.putIfAbsent("company", 2, "dn2");

        layedCachePool.putIfAbsent("goods", "1", "dn1");
        layedCachePool.putIfAbsent("goods", "2", "dn2");

        Assert.assertEquals("dn2", layedCachePool.get("2"));
        Assert.assertEquals("dn1", layedCachePool.get("1"));
        Assert.assertEquals(null, layedCachePool.get("3"));

        Assert.assertEquals("dn1", layedCachePool.get("company", 1));
        Assert.assertEquals("dn2", layedCachePool.get("company", 2));
        Assert.assertEquals(null, layedCachePool.get("company", 3));

        Assert.assertEquals("dn1", layedCachePool.get("goods", "1"));
        Assert.assertEquals("dn2", layedCachePool.get("goods", "2"));
        Assert.assertEquals(null, layedCachePool.get("goods", 3));
        CacheStatic statics = layedCachePool.getCacheStatic();
        Assert.assertEquals(statics.getItemSize(), 6);
        Assert.assertEquals(statics.getPutTimes(), 6);
        Assert.assertEquals(statics.getAccessTimes(), 9);
        Assert.assertEquals(statics.getHitTimes(), 6);
        Assert.assertTrue(statics.getLastAccesTime() > 0);
        Assert.assertTrue(statics.getLastPutTime() > 0);
        Assert.assertTrue(statics.getLastAccesTime() > 0);
        // wait expire
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        Assert.assertEquals(null, layedCachePool.get("2"));
        Assert.assertEquals(null, layedCachePool.get("1"));
        Assert.assertEquals(null, layedCachePool.get("goods", "2"));
        Assert.assertEquals(null, layedCachePool.get("company", 2));
    }

}