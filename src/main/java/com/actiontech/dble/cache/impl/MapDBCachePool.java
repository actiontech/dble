/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheStatic;
import org.mapdb.HTreeMap;

public class MapDBCachePool implements CachePool {

    private final HTreeMap<Object, Object> htreeMap;
    private final CacheStatic cacheStati = new CacheStatic();
    private final long maxSize;

    public MapDBCachePool(HTreeMap<Object, Object> htreeMap, long maxSize) {
        this.htreeMap = htreeMap;
        this.maxSize = maxSize;
        cacheStati.setMaxSize(maxSize);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        if (htreeMap.putIfAbsent(key, value) == null) {
            cacheStati.incPutTimes();
        }

    }

    @Override
    public Object get(Object key) {
        Object value = htreeMap.get(key);
        if (value != null) {
            cacheStati.incHitTimes();
            return value;
        } else {
            cacheStati.incAccessTimes();
            return null;
        }
    }

    @Override
    public void clearCache() {
        htreeMap.clear();
        cacheStati.reset();

    }

    @Override
    public CacheStatic getCacheStatic() {

        cacheStati.setItemSize(htreeMap.sizeLong());
        return cacheStati;
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }

}
