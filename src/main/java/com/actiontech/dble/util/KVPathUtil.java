/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.cluster.ClusterPathUtil;

/**
 * Created by huqing.yan on 2017/6/26.
 */
public final class KVPathUtil {
    private KVPathUtil() {
    }

    //depth:3,conf path: conf_base_path/...(detail)
    public static String getConfInitedPath() {
        return ClusterPathUtil.CONF_BASE_PATH + "inited";
    }

    //depth:3,sequences path:base_path/sequences/incr_sequence
    public static String getSequencesIncrPath() {
        return ClusterPathUtil.getSequencesPath() + ClusterPathUtil.SEPARATOR + "incr_sequence";
    }

    //depth:3,sequences path:base_path/sequences/instance
    public static String getSequencesInstancePath() {
        return ClusterPathUtil.getSequencesPath() + ClusterPathUtil.SEPARATOR + "instance";
    }


    public static String getConfInitLockPath() {
        return ClusterPathUtil.getLockBasePath() + ClusterPathUtil.SEPARATOR + "confInit.lock";
    }


    //depth:2,child node of base_path
    public static final String XALOG = ClusterPathUtil.BASE_PATH + "xalog" + ClusterPathUtil.SEPARATOR;


}
