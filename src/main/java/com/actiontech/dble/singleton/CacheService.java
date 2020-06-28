/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CachePoolFactory;
import com.actiontech.dble.cache.DefaultLayedCachePool;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.cache.impl.EnchachePoolFactory;
import com.actiontech.dble.cache.impl.LevelDBCachePooFactory;
import com.actiontech.dble.cache.impl.MapDBCachePooFactory;
import com.actiontech.dble.cache.impl.RocksDBCachePoolFactory;
import com.actiontech.dble.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * cache service for other component default using memory cache encache
 *
 * @author wuzhih
 */
public final class CacheService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheService.class);
    private static final CacheService INSTANCE = new CacheService();
    private static final String SQL_ROUTE_CACHE = "SQLRouteCache";
    private final Map<String, CachePoolFactory> poolFactories = new HashMap<>();
    private final ConcurrentMap<String, CachePool> allPools = new ConcurrentHashMap<>();

    private CacheService() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                clearCache();
            }
        });
    }

    public static CacheService getInstance() {
        return INSTANCE;
    }

    public Map<String, CachePool> getAllCachePools() {
        return this.allPools;
    }

    public void init(boolean isLowerCaseTableNames) {
        Properties props = new Properties();
        try {
            try (InputStream stream = ResourceUtil.getResourceAsStream("/cacheservice.properties")) {
                if (stream == null) {
                    LOGGER.info("cache don't be used currently! if use, please configure cacheservice.properties");
                    return;
                }
                props.load(stream);
            }
            boolean on = isSwitchOn(props);
            if (on) {
                createSpecificPool(props, isLowerCaseTableNames);
            } else {
                LOGGER.info("cache don't be used currently! if use, please switch on options in cheservice.properties");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isSwitchOn(Properties props) throws Exception {
        final String poolFactoryPref = "factory.";
        boolean use = false;

        String[] keys = props.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        for (String key : keys) {
            if (key.startsWith(poolFactoryPref)) {
                createPoolFactory(key.substring(poolFactoryPref.length()), (String) props.get(key));
                use = true;
            }
        }
        return use;
    }

    private void createSpecificPool(Properties props, boolean isLowerCaseTableNames) throws Exception {
        final String poolKeyPref = "pool.";
        final String layedPoolKeyPref = "layedpool.";

        String[] keys = props.keySet().toArray(new String[0]);
        Arrays.sort(keys);

        for (String key : keys) {
            if (key.startsWith(poolKeyPref)) {
                String cacheName = key.substring(poolKeyPref.length());
                String value = (String) props.get(key);
                String[] valueItems = value.split(",");
                if (valueItems.length < 3) {
                    throw new java.lang.IllegalArgumentException("invalid cache config, key:" + key + " value:" + value);
                }
                String type = valueItems[0];
                int size = Integer.parseInt(valueItems[1]);
                int timeOut = Integer.parseInt(valueItems[2]);
                createPool(cacheName, type, size, timeOut);
            } else if (key.startsWith(layedPoolKeyPref)) {
                String cacheName = key.substring(layedPoolKeyPref.length());
                int index = cacheName.indexOf(".");
                String parent = cacheName.substring(0, index);
                String child = cacheName.substring(index + 1);
                CachePool pool = this.allPools.get(parent);

                if (isLowerCaseTableNames) {
                    child = child.toLowerCase();
                }

                String value = (String) props.get(key);
                String[] valueItems = value.split(",");
                if (valueItems.length != 2) {
                    throw new java.lang.IllegalArgumentException("invalid primary cache config, key:" + key + " value:" + value + "too more values");
                }

                if ((pool == null) || !(pool instanceof LayerCachePool)) {
                    throw new java.lang.IllegalArgumentException("parent pool not exists or not layered cache pool:" +
                            parent + " the child cache is:" + child);
                }

                int size = Integer.parseInt(valueItems[0]);
                int timeOut = Integer.parseInt(valueItems[1]);
                ((DefaultLayedCachePool) pool).createChildCache(child, size, timeOut);
            }
        }
    }

    private void createPoolFactory(String factoryType, String factryClassName) throws Exception {
        String lowerClass = factryClassName.toLowerCase();
        switch (lowerClass) {
            case "ehcache":
                poolFactories.put(factoryType, new EnchachePoolFactory());
                break;
            case "leveldb":
                poolFactories.put(factoryType, new LevelDBCachePooFactory());
                break;
            case "mapdb":
                poolFactories.put(factoryType, new MapDBCachePooFactory());
                break;
            case "rocksdb":
                poolFactories.put(factoryType, new RocksDBCachePoolFactory());
                break;
            default:
                CachePoolFactory factry = (CachePoolFactory) Class.forName(factryClassName).newInstance();
                poolFactories.put(factoryType, factry);
        }
    }


    private void checkExists(String poolName) {
        if (allPools.containsKey(poolName)) {
            throw new java.lang.IllegalArgumentException("duplicate cache pool name: " + poolName);
        }
    }

    private CachePoolFactory getCacheFact(String type) {
        CachePoolFactory facty = this.poolFactories.get(type);
        if (facty == null) {
            throw new RuntimeException("CachePoolFactory not defined for type:" + type);
        }
        return facty;
    }

    private void createPool(String poolName, String type, int cacheSize, int expireSeconds) {
        checkExists(poolName);
        CachePoolFactory cacheFact = getCacheFact(type);
        CachePool cachePool = cacheFact.createCachePool(poolName, cacheSize, expireSeconds);
        allPools.put(poolName, cachePool);
    }

    /**
     * get cache pool by name, caller should cache result
     *
     * @param poolName poolName
     * @return CachePool
     */
    public CachePool getCachePool(String poolName) {
        return allPools.get(poolName);
    }

    public void clearCache() {
        LOGGER.info("clear all cache pool ");
        for (CachePool pool : allPools.values()) {
            pool.clearCache();
        }
    }

    public void reloadCache(boolean isLowerCaseTableNames) {
        LOGGER.info("reloadCache cache pool ");
        for (CachePool pool : allPools.values()) {
            pool.clearCache();
        }
        allPools.clear();
        init(isLowerCaseTableNames);
    }

    public static CachePool getSqlRouteCache() {
        return INSTANCE.getCachePool(SQL_ROUTE_CACHE);
    }

    public static CachePool getCachePoolByName(String poolName) {
        return INSTANCE.getCachePool(poolName);
    }
}
