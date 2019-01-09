/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cache.impl;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CachePoolFactory;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBCachePoolFactory extends CachePoolFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBCachePoolFactory.class);

    @Override
    public CachePool createCachePool(String poolName, int cacheSize, int expireSeconds) {
        final Options options = new Options();
        options.setAllowMmapReads(true).
                setAllowMmapWrites(true).
                setCreateIfMissing(true).
                setCreateMissingColumnFamilies(true);
        if (cacheSize > 0) {
            CompactionOptionsFIFO fifo = new CompactionOptionsFIFO();
            fifo.setMaxTableFilesSize(cacheSize);
            options.setCompactionOptionsFIFO(fifo);
        }

        String path = "rocksdb/" + poolName;
        try {
            RocksDB db;
            if (expireSeconds > 0) {
                db = TtlDB.open(options, path, expireSeconds, false);
            } else {
                db = RocksDB.open(options, path);
            }
            final RocksDB finalDB = db;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    FlushOptions fo = new FlushOptions();
                    fo.setWaitForFlush(true);
                    try {
                        finalDB.flush(fo);
                    } catch (RocksDBException e) {
                        LOGGER.warn("RocksDB flush error", e);
                    } finally {
                        finalDB.close();
                        fo.close();
                        options.close();
                    }
                }
            });
            return new RocksDBPool(db, poolName, cacheSize);
        } catch (RocksDBException e) {
            throw new InitStoreException(e);
        }
    }

    public static class InitStoreException extends RuntimeException {
        public InitStoreException(Throwable cause) {
            super(cause);
        }
    }
}
