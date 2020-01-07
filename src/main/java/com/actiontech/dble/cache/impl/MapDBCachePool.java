/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheStatic;
import org.mapdb.HTreeMap;

public class MapDBCachePool implements CachePool {

    private final HTreeMap<Object, Object> hTreeMap;
    private final CacheStatic cacheStatistics = new CacheStatic();
    private final long maxSize;

    public MapDBCachePool(HTreeMap<Object, Object> hTreeMap, long maxSize) {
        this.hTreeMap = hTreeMap;
        this.maxSize = maxSize;
        cacheStatistics.setMaxSize(maxSize);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        if (hTreeMap.putIfAbsent(key, value) == null) {
            cacheStatistics.incPutTimes();
        }

    }

    @Override
    public Object get(Object key) {
        Object value = hTreeMap.get(key);
        if (value != null) {
            cacheStatistics.incHitTimes();
            return value;
        } else {
            cacheStatistics.incAccessTimes();
            return null;
        }
    }

    @Override
    public void clearCache() {
        hTreeMap.clear();
        cacheStatistics.reset();

    }

    @Override
    public CacheStatic getCacheStatic() {

        cacheStatistics.setItemSize(hTreeMap.sizeLong());
        return cacheStatistics;
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }

}
