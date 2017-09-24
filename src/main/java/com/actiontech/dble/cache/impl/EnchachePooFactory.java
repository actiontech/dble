/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CachePoolFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;

public class EnchachePooFactory extends CachePoolFactory {

    @Override
    public CachePool createCachePool(String poolName, int cacheSize,
                                     int expiredSeconds) {
        CacheManager cacheManager = CacheManager.create();
        Cache enCache = cacheManager.getCache(poolName);
        if (enCache == null) {

            CacheConfiguration cacheConf = cacheManager.getConfiguration().getDefaultCacheConfiguration().clone();
            cacheConf.setName(poolName);
            if (cacheConf.getMaxEntriesLocalHeap() != 0) {
                cacheConf.setMaxEntriesLocalHeap(cacheSize);
            } else {
                cacheConf.setMaxBytesLocalHeap(String.valueOf(cacheSize));
            }
            cacheConf.setTimeToIdleSeconds(expiredSeconds);
            Cache cache = new Cache(cacheConf);
            cacheManager.addCache(cache);
            return new EnchachePool(poolName, cache, cacheSize);
        } else {
            return new EnchachePool(poolName, enCache, cacheSize);
        }
    }

}
