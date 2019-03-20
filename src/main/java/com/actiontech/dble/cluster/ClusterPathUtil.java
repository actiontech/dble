/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.config.Versions;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterPathUtil {

    public static final String UCORE_LOCAL_WRITE_PATH = "./";

    public static final String SCHEMA_SCHEMA = "schema";
    public static final String DATA_HOST = "dataHost";
    public static final String DATA_NODE = "dataNode";
    public static final String SEPARATOR = "/";
    private static final String ROOT_PATH = ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_ROOT) != null ? ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_ROOT) : "universe" + SEPARATOR + Versions.ROOT_PREFIX;

    public static final String BASE_PATH = ROOT_PATH + SEPARATOR + ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_CLUSTERID) + SEPARATOR;

    public static final String SUCCESS = "success";

    //depth:3,child node of conf_base_path
    public static final String CONF_STATUS = "status";
    public static final String SCHEMA = "schema";
    public static final String SERVER = "server";
    public static final String RULES = "rules";

    //depth:3,child node of conf_base_path
    public static String getConfSchemaPath() {
        return CONF_BASE_PATH + SCHEMA;
    }


    public static final String VERSION = "version";

    //depth:4,child node of conf_base_path/rules/
    public static final String TABLE_RULE = "tableRule";
    public static final String FUNCTION = "function";

    //depth:3,child node of conf_base_path
    public static String getConfRulePath() {
        return CONF_BASE_PATH + RULES;
    }


    //depth:4,child node of conf_base_path/server/
    public static final String DEFAULT = "default";
    public static final String USER = "user";
    public static final String FIREWALL = "firewall";
    public static final String ALARM = "alarm";

    public static String getConfServerPath() {
        return CONF_BASE_PATH + SERVER;
    }

    //depth:2,conf_base_path: base_path/conf/
    public static final String CONF_BASE_PATH = BASE_PATH + "conf" + SEPARATOR;

    //depth:2,child node of base_path
    public static final String CACHE = "cache";
    public static final String EHCACHE_NAME = "ehcache.xml";
    public static final String EHCACHE = "ehcache";

    private ClusterPathUtil() {

    }

    public static String getEhcacheNamePath() {
        return getCachePath() + SEPARATOR + EHCACHE_NAME;
    }

    public static String getEhcacheProPath() {
        return getCachePath() + SEPARATOR + "cacheservice";
    }

    //cache path base_path/cache
    public static String getCachePath() {
        return BASE_PATH + CACHE;
    }

    //depth:2,sequences path:base_path/sequences
    public static final String SEQUENCES = "sequences";

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
        return CONF_BASE_PATH + CONF_STATUS + SEPARATOR + ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
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

    //depth:2,bindata path:base_path/binlog_pause
    private static final String BINLOG_PAUSE_PATH = BASE_PATH + "conf" + SEPARATOR + "binlog_pause";
    public static final String BINLOG_PAUSE_STATUS = "status";

    public static String getBinlogPauseStatus() {
        return BINLOG_PAUSE_PATH + SEPARATOR + BINLOG_PAUSE_STATUS;
    }

    public static String getBinlogPauseStatusSelf() {
        return getBinlogPauseStatus() + SEPARATOR + ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
    }

    //depth:2,child node of base_path
    public static String getDDLPath() {
        return BASE_PATH + "ddl";
    }

    //depth:2,child node of base_path
    public static String getDDLPath(String fullName) {
        return BASE_PATH + "ddl" + SEPARATOR + fullName;
    }

    public static String getPauseDataNodePath() {
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
        return BASE_PATH + "ddl" + SEPARATOR + fullName + SEPARATOR + ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
    }

    public static String getViewPath() {
        return BASE_PATH + "view";
    }

    public static String getViewChangePath() {
        return getViewPath() + SEPARATOR + "update";
    }

}
