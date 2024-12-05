/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.cache.impl;

import com.oceanbase.obsharding_d.cache.CachePool;
import com.oceanbase.obsharding_d.cache.CachePoolFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;

public class EnchachePoolFactory extends CachePoolFactory {

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
