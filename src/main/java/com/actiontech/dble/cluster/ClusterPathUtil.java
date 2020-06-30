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

    public static final String LOCAL_WRITE_PATH = "./";

    public static final String SEPARATOR = "/";

    public static final String BASE_PATH = ClusterConfig.getInstance().getRootPath() + SEPARATOR + ClusterConfig.getInstance().getClusterId() + SEPARATOR;


    //depth:2,conf_base_path: base_path/conf/
    public static final String CONF_BASE_PATH = BASE_PATH + "conf" + SEPARATOR;

    public static final String SCHEMA = "schema";
    public static final String DB_GROUP = "dbGroup";
    public static final String SHARDING_NODE = "shardingNode";
    public static final String BLACKLIST = "blacklist";


    public static final String SUCCESS = "success";

    //depth:3,child node of conf_base_path
    private static final String CONF_STATUS = "status";
    private static final String SHARDING = "sharding";

    //depth:3,child node of conf_base_path
    public static String getConfShardingPath() {
        return CONF_BASE_PATH + SHARDING;
    }

    public static String getUserConfPath() {
        return CONF_BASE_PATH + USER;
    }
    public static String getDbConfPath() {
        return CONF_BASE_PATH + "db";
    }

    public static final String VERSION = "version";

    //depth:4,child node of conf_base_path/rules/
    public static final String FUNCTION = "function";


    public static final String USER = "user";


    private static final String DB_GROUPS = "dbGroups";
    public static final String DB_GROUP_STATUS = "dbGroup_status";
    private static final String DB_GROUP_RESPONSE = "dbGroup_response";
    private static final String DB_GROUP_LOCKS = "dbGroup_locks";

    private ClusterPathUtil() {

    }

    public static String getHaBasePath() {
        return BASE_PATH + DB_GROUPS + SEPARATOR;
    }

    public static String getHaStatusPath() {
        return getHaBasePath() + DB_GROUP_STATUS;
    }

    public static String getHaStatusPath(String dbGroupName) {
        return getHaStatusPath() + SEPARATOR + dbGroupName;
    }
    public static String getHaResponsePath() {
        return getHaBasePath() + DB_GROUP_RESPONSE;
    }
    public static String getHaResponsePath(String dhName) {
        return getHaResponsePath() + SEPARATOR + dhName;
    }
    public static String getHaLockPath(String dhName) {
        return LOCK_BASE_PATH + SEPARATOR + DB_GROUP_LOCKS + SEPARATOR + dhName;
    }
    //depth:2,sequences path:base_path/sequences
    private static final String SEQUENCES = "sequences";

    public static String getSequencesPath() {
        return CONF_BASE_PATH + SEQUENCES;
    }
    //depth:3,sequences path:base_path/sequences/common
    public static final String SEQUENCE_COMMON = "common";

    public static String getSequencesCommonPath() {
        return getSequencesPath() + SEPARATOR + SEQUENCE_COMMON;
    }

    //depth:2,lock_base_path: base_path/lock/
    private static final String LOCK_BASE_PATH = BASE_PATH + "lock";
    public static String getLockBasePath() {
        return LOCK_BASE_PATH;
    }
    //depth:3, lock path : lock_base_path/...(detail)
    public static String getSyncMetaLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "syncMeta.lock";
    }

    public static String getConfChangeLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "confChange.lock";
    }

    public static String getConfStatusPath() {
        return CONF_BASE_PATH + CONF_STATUS;
    }

    public static String getConfStatusOperatorPath() {
        return CONF_BASE_PATH + CONF_STATUS + SEPARATOR + "operator";
    }

    //depth:2,child node of base_path
    public static String getOnlinePath() {
        return BASE_PATH + "online";
    }

    public static String getOnlinePath(String instanceName) {
        return getOnlinePath() + SEPARATOR + instanceName;
    }

    //depth:2,binlog_pause path:base_path/binlog_pause
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


    //depth:2,child node of base_path
    public static String getDDLPath() {
        return BASE_PATH + "ddl";
    }

    //depth:2,child node of base_path
    public static String getDDLPath(String fullName) {
        return getDDLPath() + SEPARATOR + fullName;
    }

    //depth:2,child node of base_path
    public static String getDDLLockPath(String fullName) {
        return LOCK_BASE_PATH + SEPARATOR + "ddl_lock" + SEPARATOR + fullName;
    }

    public static String getPauseShardingNodePath() {
        return CONF_BASE_PATH + "migration";
    }

    public static String getPauseResultNodePath() {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "pause";
    }

    public static String getPauseResultNodePath(String id) {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "pause" + SEPARATOR + id;
    }


    public static String getPauseResumePath() {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "resume";
    }

    public static String getPauseResumePath(String id) {
        return CONF_BASE_PATH + "migration" + SEPARATOR + "resume" + SEPARATOR + id;
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

    public static String getViewLockPath(String schemaName, String viewName) {
        return LOCK_BASE_PATH + "view_lock" + schemaName + SCHEMA_VIEW_SPLIT + viewName;
    }
}
