/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cache.impl;

import com.oceanbase.obsharding_d.cache.CachePool;
import com.oceanbase.obsharding_d.cache.CachePoolFactory;
import org.rocksdb.*;

public class RocksDBCachePoolFactory extends CachePoolFactory {

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
            return new RocksDBPool(db, options, poolName, cacheSize);
        } catch (RocksDBException e) {
            throw new InitStoreException(e);
        }
    }

    public static class InitStoreException extends RuntimeException {
        InitStoreException(Throwable cause) {
            super(cause);
        }
    }
}
