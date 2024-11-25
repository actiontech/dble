/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public enum ChangeType {
    /**
     * cluster event type
     */
    ADDED,
    /*
     * UPDATED event is discarded in order to prevent event merge.
     * This UPDATED event are split into two event, remove the old and add the new.
     */
    //    @Deprecated UPDATED,
    /**
     * cluster event type
     */
    REMOVED;
}
