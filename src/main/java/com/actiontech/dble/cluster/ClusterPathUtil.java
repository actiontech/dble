/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterPathUtil {

    public static final String UCORE_LOCAL_WRITE_PATH = "./";

    public static final String SCHEMA = "schema";
    public static final String DB_GROUP = "dbGroup";
    public static final String SHARDING_NODE = "shardingNode";
    public static final String BLACKLIST = "blacklist";
    public static final String SEPARATOR = "/";
    public static final String BASE_PATH = ClusterConfig.getInstance().getRootPath() + SEPARATOR + ClusterConfig.getInstance().getClusterID() + SEPARATOR;

    public static final String SUCCESS = "success";

    //depth:3,child node of conf_base_path
    private static final String CONF_STATUS = "status";
    private static final String SHARDING = "sharding";
    private static final String SERVER = "server";
    public static final String DBS = "db";

    //depth:3,child node of conf_base_path
    public static String getConfShardingPath() {
        return CONF_BASE_PATH + SHARDING;
    }

    public static String getConfUserPath() {
        return CONF_BASE_PATH + USER;
    }
    public static String getDbConfPath() {
        return CONF_BASE_PATH + DBS;
    }

    public static final String VERSION = "version";

    //depth:4,child node of conf_base_path/rules/
    public static final String FUNCTION = "function";


    //depth:4,child node of conf_base_path/server/
    public static final String DEFAULT = "default";
    public static final String USER = "user";
    public static final String FIREWALL = "firewall";
    //public static final String ALARM = "alarm";

    public static String getConfServerPath() {
        return CONF_BASE_PATH + SERVER;
    }

    //depth:2,conf_base_path: base_path/conf/
    public static final String CONF_BASE_PATH = BASE_PATH + "conf" + SEPARATOR;

    //depth:2,child node of base_path
    private static final String CACHE = "cache";
    private static final String DB_GROUPS = "dbGroups";
    public static final String DB_GROUP_STATUS = "dbGroup_status";
    private static final String DB_GROUP_LOCKS = "dbGroup_locks";
    private static final String EHCACHE_NAME = "ehcache.xml";
    public static final String EHCACHE = "ehcache";

    private ClusterPathUtil() {

    }

    public static String getEhcacheNamePath() {
        return getCachePath() + SEPARATOR + EHCACHE_NAME;
    }

    public static String getEhcacheProPath() {
        return getCachePath() + SEPARATOR + "cacheservice";
    }

    public static String getHaBasePath() {
        return BASE_PATH + DB_GROUPS + SEPARATOR;
    }

    public static String getHaStatusPath() {
        return BASE_PATH + DB_GROUPS + SEPARATOR + DB_GROUP_STATUS + SEPARATOR;
    }

    public static String getHaStatusPath(String dhName) {
        return getHaStatusPath() + dhName;
    }

    public static String getHaLockPath(String dhName) {
        return BASE_PATH + DB_GROUPS + SEPARATOR + DB_GROUP_LOCKS + SEPARATOR + dhName;
    }

    public static String getSelfResponsePath(String notifyPath) {
        return notifyPath + SEPARATOR + SystemConfig.getInstance().getInstanceId();
    }

    //cache path base_path/cache
    private static String getCachePath() {
        return BASE_PATH + CACHE;
    }

    //depth:2,sequences path:base_path/sequences
    private static final String SEQUENCES = "sequences";

    public static String getSequencesPath() {
        return CONF_BASE_PATH + SEQUENCES;
    }


    //depth:2,lock_base_path: base_path/lock/
    private static final String LOCK_BASE_PATH = CONF_BASE_PATH + "lock";

    public static String getConfChangeLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "confChange.lock";
    }

    public static String getConfStatusPath() {
        return CONF_BASE_PATH + CONF_STATUS;
    }

    public static String getSelfConfStatusPath() {
        return CONF_BASE_PATH + CONF_STATUS + SEPARATOR + SystemConfig.getInstance().getInstanceId();
    }

    //depth:2,child node of base_path
    public static String getOnlinePath() {
        return BASE_PATH + "online";
    }

    public static String getOnlinePath(String serverId) {
        return BASE_PATH + "online" + SEPARATOR + serverId;
    }

    public static String getBinlogPauseLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "binlogStatus.lock";
    }

    //depth:2,binlog_pause path:base_path/binlog_pause
    private static final String BINLOG_PAUSE_PATH = BASE_PATH + "conf" + SEPARATOR + "binlog_pause";
    private static final String BINLOG_PAUSE_STATUS = "status";

    public static String getBinlogPauseStatus() {
        return BINLOG_PAUSE_PATH + SEPARATOR + BINLOG_PAUSE_STATUS;
    }

    public static String getBinlogPauseStatusSelf() {
        return getBinlogPauseStatus() + SEPARATOR + SystemConfig.getInstance().getInstanceId();
    }

    //depth:2,child node of base_path
    public static String getDDLPath() {
        return BASE_PATH + "ddl";
    }

    //depth:2,child node of base_path
    public static String getDDLPath(String fullName) {
        return BASE_PATH + "ddl" + SEPARATOR + fullName;
    }

    //depth:2,child node of base_path
    public static String getDDLLockPath(String fullName) {
        return BASE_PATH + "ddl_lock" + SEPARATOR + fullName;
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

    //depth:2,child node of base_path
    public static String getDDLInstancePath(String fullName) {
        return BASE_PATH + "ddl" + SEPARATOR + fullName + SEPARATOR + SystemConfig.getInstance().getInstanceId();
    }

    public static String getViewPath() {
        return BASE_PATH + "view";
    }

    public static String getViewChangePath() {
        return getViewPath() + SEPARATOR + "update";
    }

}
