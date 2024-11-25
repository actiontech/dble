/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d;

import com.oceanbase.obsharding_d.cache.CacheStatic;
import com.oceanbase.obsharding_d.cache.LayerCachePool;

import java.util.HashMap;
import java.util.Map;

public class SimpleCachePool implements LayerCachePool {
    private HashMap<Object, Object> cacheMap = new HashMap<Object, Object>();

    @Override
    public void putIfAbsent(Object key, Object value) {
        cacheMap.put(key, value);

    }

    @Override
    public Object get(Object key) {
        return cacheMap.get(key);
    }

    @Override
    public void clearCache() {
        cacheMap.clear();

    }

    @Override
    public CacheStatic getCacheStatic() {
        return null;
    }

    @Override
    public void putIfAbsent(String primaryKey, Object secondKey, Object value) {
        putIfAbsent(primaryKey + "_" + secondKey, value);

    }

    @Override
    public Object get(String primaryKey, Object secondKey) {
        return get(primaryKey + "_" + secondKey);
    }

    @Override
    public Map<String, CacheStatic> getAllCacheStatic() {

        return null;
    }

    @Override
    public long getMaxSize() {
        return 100;
    }
};