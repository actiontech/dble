/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CachePoolFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.TimeUnit;

public class MapDBCachePooFactory extends CachePoolFactory {
    private DB db = DBMaker.newMemoryDirectDB().cacheSize(1000).cacheLRUEnable().make();

    @Override
    public CachePool createCachePool(String poolName, int cacheSize,
                                     int expiredSeconds) {

        HTreeMap<Object, Object> cache = this.db.createHashMap(poolName).
                expireMaxSize(cacheSize).
                expireAfterAccess(expiredSeconds, TimeUnit.SECONDS).
                makeOrGet();
        return new MapDBCachePool(cache, cacheSize);

    }

}
