package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheStatic;
import org.nustaq.serialization.FSTConfiguration;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBPool implements CachePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBPool.class);
    private FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();
    private final RocksDB cache;
    private final CacheStatic cacheStatistics = new CacheStatic();
    private final String name;
    private final long maxSize;

    public RocksDBPool(RocksDB cache, String name, long maxSize) {
        this.cache = cache;
        this.name = name;
        this.maxSize = maxSize;
    }

    @Override
    public void putIfAbsent(Object key, Object value) {
        try{
            cache.put(fst.asByteArray(key), fst.asByteArray(value));
            cacheStatistics.incPutTimes();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(name + " add leveldb cache ,key:" + key + " value:" + value);
            }
        }catch(Exception e){
            throw new RocksDBPoolException(e);
        }
    }

    @Override
    public Object get(Object key) {
        try{
            byte[] keyBytes = fst.asByteArray(key);
            byte[] bytes = cache.get(fst.asByteArray(keyBytes));
            if (bytes != null) {
                Object cached = fst.asObject(bytes);

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(name + " hit cache ,key:" + key);
                }
                cacheStatistics.incHitTimes();
                return cached;
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(name + "  miss cache ,key:" + key);
                }
                cacheStatistics.incAccessTimes();
                return null;
            }
        }catch(Exception e){
            throw new RocksDBPoolException(e);
        }
    }

    @Override
    public void clearCache() {
        LOGGER.info("clear cache " + name);
        //cache.delete(key);
        cacheStatistics.reset();
        //cacheStati.setMemorySize(cache.g);
    }

    @Override
    public CacheStatic getCacheStatic() {
        cacheStatistics.setItemSize(cacheStatistics.getPutTimes());
        return cacheStatistics;
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }
    public static class RocksDBPoolException extends RuntimeException{
        public RocksDBPoolException(Throwable cause) {
            super(cause);
        }
    }
}
