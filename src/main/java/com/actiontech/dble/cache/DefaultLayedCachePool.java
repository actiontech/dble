/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultLayedCachePool implements LayerCachePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLayedCachePool.class);
    protected Map<String, CachePool> allCaches = new HashMap<>();
    protected final ReentrantLock lock = new ReentrantLock();
    protected int defaultCacheSize;
    protected int defaultExpiredSeconds;
    protected static final String DEFAULT_CACHE = "default";
    private final CachePoolFactory poolFactory;
    private final String name;
    private final boolean hasDefaultCache;

    public DefaultLayedCachePool(String name, CachePoolFactory poolFactory, int defaultCacheSize, int defaultExpiredSeconds) {
        super();
        this.name = name;
        this.poolFactory = poolFactory;
        this.defaultCacheSize = defaultCacheSize;
        this.defaultExpiredSeconds = defaultExpiredSeconds;
        this.hasDefaultCache = defaultCacheSize != 0;
    }

    private CachePool getCache(String cacheName) {
        CachePool pool = allCaches.get(cacheName);
        if ((pool == null) && (hasDefaultCache)) {
            lock.lock();
            try {
                pool = allCaches.get(cacheName);
                if (pool == null) {
                    pool = this.createChildCache(cacheName, this.defaultCacheSize, this.defaultExpiredSeconds);
                }
            } finally {
                lock.unlock();
            }
        }

        return pool;
    }

    /**
     * create child cache at runtime
     *
     * @param cacheName
     * @return
     */
    public CachePool createChildCache(String cacheName, int size, int expireSeconds) {
        LOGGER.info("create child Cache: " + cacheName + " for layered cache " + name + ", size " + size + ", expire seconds " + expireSeconds);
        CachePool child = this.poolFactory.createCachePool(name + "." + cacheName, size, expireSeconds);
        allCaches.put(cacheName, child);
        return child;
    }

    /* obsoleted, to be clean */
    @Override
    public void putIfAbsent(Object key, Object value) {
        putIfAbsent(DEFAULT_CACHE, key, value);
    }

    @Override
    public void putIfAbsent(String cacheKey, Object secondKey, Object value) {
        CachePool pool = getCache(cacheKey);
        if (pool != null) {
            pool.putIfAbsent(secondKey, value);
        }
    }

    /* obsoleted, to be clean */
    @Override
    public Object get(Object key) {
        return get(DEFAULT_CACHE, key);
    }

    @Override
    public Object get(String cacheKey, Object secondKey) {
        CachePool pool = getCache(cacheKey);
        if (pool != null) {
            return pool.get(secondKey);
        } else {
            return null;
        }
    }

    @Override
    public void clearCache() {
        LOGGER.info("clear cache ");
        for (CachePool pool : allCaches.values()) {
            pool.clearCache();
        }
    }

    @Override
    public CacheStatic getCacheStatic() {
        CacheStatic cacheStatic = new CacheStatic();
        cacheStatic.setMaxSize(this.getMaxSize());
        for (CacheStatic singleStatic : getAllCacheStatic().values()) {
            cacheStatic.setItemSize(cacheStatic.getItemSize() + singleStatic.getItemSize());
            cacheStatic.setHitTimes(cacheStatic.getHitTimes() + singleStatic.getHitTimes());
            cacheStatic.setAccessTimes(cacheStatic.getAccessTimes() + singleStatic.getAccessTimes());
            cacheStatic.setPutTimes(cacheStatic.getPutTimes() + singleStatic.getPutTimes());
            if (cacheStatic.getLastAccessTime() < singleStatic.getLastAccessTime()) {
                cacheStatic.setLastAccessTime(singleStatic.getLastAccessTime());
            }
            if (cacheStatic.getLastPutTime() < singleStatic.getLastPutTime()) {
                cacheStatic.setLastPutTime(singleStatic.getLastPutTime());
            }
        }
        return cacheStatic;
    }

    @Override
    public Map<String, CacheStatic> getAllCacheStatic() {
        Map<String, CacheStatic> results = new HashMap<>(this.allCaches.size());
        for (Map.Entry<String, CachePool> entry : allCaches.entrySet()) {
            results.put(entry.getKey(), entry.getValue().getCacheStatic());
        }
        return results;
    }

    @Override
    public long getMaxSize() {
        long maxSize = 0;
        for (CachePool cache : this.allCaches.values()) {
            maxSize += cache.getMaxSize();
        }
        return maxSize;
    }
}
