/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

import com.actiontech.dble.cache.impl.EnchachePool;
import com.actiontech.dble.cache.impl.MapDBCachePooFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;

/**
 * test cache performance ,for encache test set  VM param  -server -Xms1100M -Xmx1100M
 * for mapdb set vm param -server -Xms100M -Xmx100M -XX:MaxPermSize=1G
 */

public class TestCachePoolPerformance {
    private CachePool pool;
    private int maxCacheCount = 100 * 10000;

    public static CachePool createEnCachePool() {
        CacheConfiguration cacheConf = new CacheConfiguration();
        cacheConf.setName("testcache");
        cacheConf.maxBytesLocalHeap(400, MemoryUnit.MEGABYTES)
                .timeToIdleSeconds(3600);
        Cache cache = new Cache(cacheConf);
        CacheManager.create().addCache(cache);
        EnchachePool enCachePool = new EnchachePool(cacheConf.getName(), cache, 400 * 10000);
        return enCachePool;
    }

    public static CachePool createMapDBCachePool() {
        MapDBCachePooFactory fact = new MapDBCachePooFactory();
        return fact.createCachePool("mapdbcache", 100 * 10000, 3600);

    }

    public void test() {
        testSwarm();
        testInsertSpeed();
        testSelectSpeed();
    }

    private void testSwarm() {
        System.out.println("prepare ........");
        for (int i = 0; i < 100000; i++) {
            pool.putIfAbsent(i % 100, "dn1");
        }
        for (int i = 0; i < 100000; i++) {
            pool.get(i % 100);
        }
        pool.clearCache();
    }

    private void testSelectSpeed() {
        System.out.println("test select speed for " + this.pool + " count:"
                + this.maxCacheCount);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < maxCacheCount; i++) {
            pool.get(i + "");
        }
        double used = (System.currentTimeMillis() - startTime) / 1000.0;
        CacheStatic statics = pool.getCacheStatic();
        System.out.println("used time:" + used + " tps:" + maxCacheCount / used
                + " cache hit:" + 100 * statics.getHitTimes()
                / statics.getAccessTimes());
    }

    private void GC() {
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
    }

    private void testInsertSpeed() {
        this.GC();
        long freeMem = Runtime.getRuntime().freeMemory();
        System.out.println("test insert speed for " + this.pool
                + " with insert count:" + this.maxCacheCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < maxCacheCount; i++) {
            try {
                pool.putIfAbsent(i + "", "dn" + i % 100);
            } catch (Error e) {
                System.out.println("insert " + i + " error");
                e.printStackTrace();
                break;
            }
        }
        long used = (System.currentTimeMillis() - start) / 1000;
        long count = pool.getCacheStatic().getItemSize();
        this.GC();
        long usedMem = freeMem - Runtime.getRuntime().freeMemory();
        System.out.println(" cache size is " + count + " ,all in cache :"
                + (count == maxCacheCount) + " ,used time:" + used + " ,tps:"
                + count / used + " used memory:" + usedMem / 1024 / 1024 + "M");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out
                    .println("usage : \r\n cache: 1 for encache 2 for mapdb\r\n");
            return;
        }
        TestCachePoolPerformance tester = new TestCachePoolPerformance();
        int cacheType = Integer.parseInt(args[0]);
        if (cacheType == 1) {
            tester.pool = createEnCachePool();
            tester.test();
        } else if (cacheType == 2) {
            tester.pool = createMapDBCachePool();
            tester.test();
        } else {
            System.out.println("not valid input ");
        }

    }
}