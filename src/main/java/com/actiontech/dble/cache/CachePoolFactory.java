/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

/**
 * factory used to create cachePool
 *
 * @author wuzhih
 */
public abstract class CachePoolFactory {

    /**
     * create a cache pool instance
     *
     * @param poolName
     * @param cacheSize
     * @param expireSeconds -1 for not expired
     * @return
     */
    public abstract CachePool createCachePool(String poolName, int cacheSize, int expireSeconds);
}
