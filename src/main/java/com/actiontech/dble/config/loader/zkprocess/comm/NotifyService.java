/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.comm;

/**
 * @author liujun
 * @date 2015/2/4
 * @vsersion 0.0.1
 */
public interface NotifyService {

    /**
     * notify interface
     *
     * @return true for success ,false for failed
     * @throws Exception
     */
    boolean notifyProcess() throws Exception;
}
