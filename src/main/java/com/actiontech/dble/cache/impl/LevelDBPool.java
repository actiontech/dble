/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cache.impl;


import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheStatic;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class LevelDBPool implements CachePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBPool.class);
    private final DB cache;
    private final CacheStatic cacheStatistics = new CacheStatic();
    private final String name;
    private final long maxSize;

    public LevelDBPool(String name, DB db, long maxSize) {
        this.cache = db;
        this.name = name;
        this.maxSize = maxSize;
        cacheStatistics.setMaxSize(maxSize);
    }

    @Override
    public void putIfAbsent(Object key, Object value) {

        cache.put(toByteArray(key), toByteArray(value));
        cacheStatistics.incPutTimes();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(name + " add leveldb cache ,key:" + key + " value:" + value);
        }
    }

    @Override
    public Object get(Object key) {

        Object ob = toObject(cache.get(toByteArray(key)));
        if (ob != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(name + " hit cache ,key:" + key);
            }
            cacheStatistics.incHitTimes();
            return ob;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(name + "  miss cache ,key:" + key);
            }
            cacheStatistics.incAccessTimes();
            return null;
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

        /*
        int i=0;
        try {
         // DBIterator iterator = cache.iterator();
          for(cache.iterator().seekToFirst(); cache.iterator().hasNext(); cache.iterator().next()) {
              i++;
          }
          cache.iterator().close();
        } catch (Exception e) {
              // Make sure you close the iterator to avoid resource leaks.
        }
        //long[] sizes = cache.getApproximateSizes(new Range(bytes("TESTDB"), bytes("TESTDC")));
         */
        //cacheStati.setItemSize(cache.getSize());//sizes[0]);
        cacheStatistics.setItemSize(cacheStatistics.getPutTimes());
        return cacheStatistics;
    }

    @Override
    public long getMaxSize() {

        return maxSize;
    }

    public byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            LOGGER.info("toByteArrayError", ex);
        }
        return bytes;
    }


    public Object toObject(byte[] bytes) {
        Object obj = null;
        if ((bytes == null) || (bytes.length <= 0)) {
            return obj;
        }
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            LOGGER.info("toObjectError", ex);
        } catch (ClassNotFoundException ex) {
            LOGGER.info("toObjectError", ex);
        }
        return obj;
    }

}
