package io.mycat.cache.impl;


import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.cache.CachePool;
import io.mycat.cache.CachePoolFactory;

public class LevelDBCachePooFactory extends CachePoolFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBCachePooFactory.class);

    @Override
    public CachePool createCachePool(String poolName, int cacheSize,
                                     int expireSeconds) {
        Options options = new Options();
        options.cacheSize(1048576L * cacheSize); //cacheSize M
        options.createIfMissing(true);
        DB db = null;
        String filePath = "leveldb\\" + poolName;
        try {
            db = factory.open(new File(filePath), options);
            // Use the db in here....
        } catch (IOException e) {
            LOGGER.info("factory try to open file " + filePath + " failed ");
            // Make sure you close the db to shutdown the
            // database and avoid resource leaks.
            // db.close();
        }
        return new LevelDBPool(poolName, db, cacheSize);
    }

}
