/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import java.util.List;

/**
 * @author dcy
 * Create Date: 2025-04-17
 */
public interface ITraceResult {
    public enum SqlTraceType {
        SINGLE_NODE_QUERY, MULTI_NODE_QUERY, MULTI_NODE_GROUP, COMPLEX_QUERY, RWSPLIT_QUERY;
    }

    boolean isCompleted();

    RwTraceResult.SqlTraceType getType();

    List<String[]> genLogResult();

    double getOverAllMilliSecond();

    String getOverAllSecond();
}
