/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public enum ChangeType {
    /**
     * cluster event type
     */
    ADDED,
    /**
     * cluster event type.
     * May cause some event missing.Should  use it  carefully.
     */
    @Deprecated UPDATED,
    /**
     * cluster event type
     */
    REMOVED;
}
