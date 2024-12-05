/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.cache;

/**
 * simple cache pool for implement
 *
 * @author wuzhih
 */
public interface CachePool {

    void putIfAbsent(Object key, Object value);

    Object get(Object key);

    void clearCache();

    CacheStatic getCacheStatic();

    long getMaxSize();
}
