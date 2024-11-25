/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;

/**
 * @author dcy
 * Create Date: 2021-03-31
 */
public interface GeneralListener<T> {
    GeneralListener<T> registerPrefixForUcore(ClusterClearKeyListener confListener);

    /**
     * @param event
     */
    void onEvent(ClusterEvent<T> event) throws Exception;

    /**
     * only support zk yet.
     *
     * @throws Exception
     */
    default void onInit() throws Exception {
        return;
    }

    GeneralListener<T> registerPrefixForZk();
}
