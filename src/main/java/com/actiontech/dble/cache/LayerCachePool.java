/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

import java.util.Map;

/**
 * Layered cache pool
 *
 * @author wuzhih
 */
public interface LayerCachePool extends CachePool {

    void putIfAbsent(String cacheKey, Object secondKey, Object value);

    Object get(String cacheKey, Object secondKey);

    /**
     * get all cache static, name is cache name
     *
     * @return map of CacheStatic
     */
    Map<String, CacheStatic> getAllCacheStatic();
}
