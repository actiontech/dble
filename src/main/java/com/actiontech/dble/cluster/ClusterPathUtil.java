/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.config.model.ClusterConfig;

import static com.actiontech.dble.backend.mysql.view.Repository.SCHEMA_VIEW_SPLIT;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterPathUtil {
    private ClusterPathUtil() {
    }

    public static final String LOCAL_WRITE_PATH = "./";

    public static final String SEPARATOR = "/";

    public static final String BASE_PATH = ClusterConfig.getInstance().getRootPath() + SEPARATOR + ClusterConfig.getInstance().getClusterId() + SEPARATOR;


    //depth:2,conf_base_path: base_path/conf/
    public static final String CONF_BASE_PATH = BASE_PATH + "conf" + SEPARATOR;

    //xml properties
    static final String SCHEMA = "schema";
    static final String DB_GROUP = "dbGroup";
    static final String SHARDING_NODE = "shardingNode";
    static final String BLACKLIST = "blacklist";
    static final String VERSION = "version";
    static final String FUNCTION = "function";
    static final String USER = "user";


    public static final String SUCCESS = "success";
    public static final String DB_GROUP_STATUS = "dbGroup_status";

    public static String getConfShardingPath() {
        return CONF_BASE_PATH + "sharding";
    }

    public static String getUserConfPath() {
        return CONF_BASE_PATH + "user";
    }

    public static String getDbConfPath() {
        return CONF_BASE_PATH + "db";
    }


    public static String getHaBasePath() {
        return BASE_PATH + "dbGroups" + SEPARATOR;
    }

    public static String getHaStatusPath() {
        return getHaBasePath() + DB_GROUP_STATUS;
    }

    public static String getHaStatusPath(String dbGroupName) {
        return getHaStatusPath() + SEPARATOR + dbGroupName;
    }

    public static String getHaResponsePath() {
        return getHaBasePath() + "dbGroup_response";
    }

    public static String getHaResponsePath(String dhName) {
        return getHaResponsePath() + SEPARATOR + dhName;
    }

    public static String getHaLockPath(String dhName) {
        return LOCK_BASE_PATH + SEPARATOR + "dbGroup_locks" + SEPARATOR + dhName;
    }

    //depth:2,sequences path:base_path/sequences
    private static final String SEQUENCES = "sequences";

    public static String getSequencesPath() {
        return CONF_BASE_PATH + SEQUENCES;
    }
    //depth:3,sequences path:base_path/sequences/common

    public static String getSequencesCommonPath() {
        return getSequencesPath() + SEPARATOR + "common";
    }

    private static final String LOCK_BASE_PATH = BASE_PATH + "lock";

    public static String getLockBasePath() {
        return LOCK_BASE_PATH;
    }

    public static String getSyncMetaLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "syncMeta.lock";
    }

    public static String getConfChangeLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "confChange.lock";
    }

    public static String getConfStatusPath() {
        return CONF_BASE_PATH + "status";
    }

    public static String getConfStatusOperatorPath() {
        return getConfStatusPath() + SEPARATOR + "operator";
    }

    public static String getOnlinePath() {
        return BASE_PATH + "online";
    }

    public static String getOnlinePath(String instanceName) {
        return getOnlinePath() + SEPARATOR + instanceName;
    }

    private static final String BINLOG_PAUSE_PATH = BASE_PATH + "binlog_pause";
    private static final String BINLOG_PAUSE_STATUS = "status";


    public static String getBinlogPauseLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "binlogStatus.lock";
    }

    public static String getBinlogPause() {
        return BINLOG_PAUSE_PATH;
    }

    public static String getBinlogPauseStatus() {
        return BINLOG_PAUSE_PATH + SEPARATOR + BINLOG_PAUSE_STATUS;
    }


    public static String getDDLPath() {
        return BASE_PATH + "ddl";
    }

    public static String getDDLPath(String fullName) {
        return getDDLPath() + SEPARATOR + fullName;
    }


    public static String getDDLLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "ddl_lock";
    }

    public static String getDDLLockPath(String fullName) {
        return getDDLLockPath() + SEPARATOR + fullName;
    }

    public static String getPauseShardingNodePath() {
        return CONF_BASE_PATH + "migration";
    }

    public static String getPauseShardingNodeLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "pause_node.lock";
    }

    public static String getPauseResultNodePath() {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "pause";
    }


    public static String getPauseResumePath() {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "resume";
    }


    public static String getViewPath() {
        return BASE_PATH + "view";
    }

    public static String getViewPath(String schemaName, String viewName) {
        return ClusterPathUtil.getViewPath() + SEPARATOR + schemaName + SCHEMA_VIEW_SPLIT + viewName;
    }

    public static String getViewChangePath() {
        return getViewPath() + SEPARATOR + "operator";
    }

    public static String getViewChangePath(String schemaName, String viewName) {
        return getViewChangePath() + SEPARATOR + schemaName + SCHEMA_VIEW_SPLIT + viewName;
    }

    public static String getViewLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "view_lock";
    }
    public static String getViewLockPath(String schemaName, String viewName) {
        return getViewLockPath() + SEPARATOR + schemaName + SCHEMA_VIEW_SPLIT + viewName;
    }
}
