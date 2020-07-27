/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ToResolveContainer {
    private ToResolveContainer() {
    }

    public static final Set<String> TABLE_NOT_CONSISTENT_IN_SHARDINGS = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> TABLE_NOT_CONSISTENT_IN_MEMORY = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> TABLE_LACK = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> GLOBAL_TABLE_CONSISTENCY = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> SHARDING_NODE_LACK = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> CREATE_CONN_FAIL = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> REACH_MAX_CON = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> XA_WRITE_CHECK_POINT_FAIL = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    public static final Set<String> DB_INSTANCE_LOWER_CASE_ERROR = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
}
