/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;

/**
 * Created by huqing.yan on 2017/6/26.
 */
public final class KVPathUtil {
    private KVPathUtil() {
    }

    public static final String SEPARATOR = "/";
    private static final String ROOT_PATH = Versions.ROOT_PREFIX;
    //depth:1, base_path" /root_name/cluster_name/
    public static final String BASE_PATH = SEPARATOR + ROOT_PATH + SEPARATOR + ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_CLUSTERID) + SEPARATOR;

    //depth:2,child node of base_path
    public static final String CACHE = "cache";

    //cache path base_path/cache
    public static String getCachePath() {
        return BASE_PATH + CACHE;
    }

    //depth:3,child node of base_path/cache/
    public static final String CACHESERVER_NAME = "cacheservice.properties";
    public static final String EHCACHE_NAME = "ehcache.xml";

    public static String getCacheServerNamePath() {
        return getCachePath() + SEPARATOR + CACHESERVER_NAME;
    }

    public static String getEhcacheNamePath() {
        return getCachePath() + SEPARATOR + EHCACHE_NAME;
    }

    //depth:2,child node of base_path
    public static String getOnlinePath() {
        return BASE_PATH + "online";
    }

    //depth:2,child node of base_path
    public static String getDDLPath() {
        return BASE_PATH + "ddl";
    }

    public static String getViewPath() {
        return BASE_PATH + "view";
    }

    //depth:4,grandson node of base_path/ddl/
    public static final String DDL_INSTANCE = "instance";

    //depth:2,bindata path:base_path/bindata
    public static String getBinDataPath() {
        return BASE_PATH + "bindata";
    }

    //depth:2,bindata path:base_path/binlog_pause
    private static final String BINLOG_PAUSE_PATH = BASE_PATH + "binlog_pause";

    //depth:3,child node of binlog_pause:base_path/binlog_pause/
    public static String getBinlogPauseInstance() {
        return BINLOG_PAUSE_PATH + SEPARATOR + "instance";
    }

    public static final String BINLOG_PAUSE_STATUS = "status";

    public static String getBinlogPauseStatus() {
        return BINLOG_PAUSE_PATH + SEPARATOR + BINLOG_PAUSE_STATUS;
    }

    //depth:2,conf_base_path: base_path/conf/
    private static final String CONF_BASE_PATH = BASE_PATH + "conf" + SEPARATOR;

    //depth:3,conf path: conf_base_path/...(detail)
    public static String getConfInitedPath() {
        return CONF_BASE_PATH + "inited";
    }

    //depth:3,child node of conf_base_path
    public static final String CONF_STATUS = "status";
    public static final String SCHEMA = "schema";
    public static final String SERVER = "server";
    public static final String RULES = "rules";

    public static String getConfStatusPath() {
        return CONF_BASE_PATH + CONF_STATUS;
    }

    //depth:3,child node of conf_base_path
    public static String getConfSchemaPath() {
        return CONF_BASE_PATH + SCHEMA;
    }

    //depth:4,child node of conf_base_path/schema/
    public static final String SCHEMA_SCHEMA = "schema";
    public static final String DATA_HOST = "dataHost";
    public static final String DATA_NODE = "dataNode";

    //depth:3,child node of conf_base_path
    public static String getConfServerPath() {
        return CONF_BASE_PATH + SERVER;
    }

    //depth:4,child node of conf_base_path/server/
    public static final String DEFAULT = "default";
    public static final String USER = "user";
    public static final String FIREWALL = "firewall";

    //depth:3,child node of conf_base_path
    public static String getConfRulePath() {
        return CONF_BASE_PATH + RULES;
    }

    //depth:4,child node of conf_base_path/rules/
    public static final String TABLE_RULE = "tableRule";
    public static final String FUNCTION = "function";

    public static final String VERSION = "version";

    //depth:2,sequences path:base_path/sequences
    public static final String SEQUENCES = "sequences";

    public static final String DATA_HOSTS = "data_hosts";
    public static final String DATA_HOST_RESPONSE = "data_host_response";
    public static final String DATA_HOST_STATUS = "data_host_status";
    public static final String DATA_HOST_LOCKS = "data_host_locks";

    public static String getSequencesPath() {
        return BASE_PATH + SEQUENCES;
    }

    //depth:3,sequences path:base_path/sequences/incr_sequence
    public static String getSequencesIncrPath() {
        return getSequencesPath() + SEPARATOR + "incr_sequence";
    }

    //depth:3,sequences path:base_path/sequences/leader
    public static String getSequencesLeaderPath() {
        return getSequencesPath() + SEPARATOR + "leader";
    }

    //depth:3,sequences path:base_path/sequences/common
    public static final String SEQUENCE_COMMON = "common";

    public static String getSequencesCommonPath() {
        return getSequencesPath() + SEPARATOR + SEQUENCE_COMMON + SEPARATOR;
    }

    //depth:3,sequences path:base_path/sequences/instance
    public static String getSequencesInstancePath() {
        return getSequencesPath() + SEPARATOR + "instance";
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

    public static String getDnIndexLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "dnindex.lock";
    }

    public static String getConfInitLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "confInit.lock";
    }

    public static String getConfChangeLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "confChange.lock";
    }

    public static String getBinlogPauseLockPath() {
        return LOCK_BASE_PATH + SEPARATOR + "binlogStatus.lock";
    }

    //depth:2,child node of base_path
    public static final String XALOG = BASE_PATH + "xalog" + SEPARATOR;

    public static String getHaBasePath() {
        return BASE_PATH + DATA_HOSTS + SEPARATOR;
    }

    public static String getHaStatusPath() {
        return BASE_PATH + DATA_HOSTS + SEPARATOR + DATA_HOST_STATUS;
    }

    public static String getHaStatusPath(String dhName) {
        return getHaStatusPath() + SEPARATOR + dhName;
    }

    public static String getHaResponsePath() {
        return BASE_PATH + DATA_HOSTS + SEPARATOR + DATA_HOST_RESPONSE;
    }

    public static String getHaResponsePath(String dhName) {
        return BASE_PATH + DATA_HOSTS + SEPARATOR + DATA_HOST_RESPONSE + SEPARATOR + dhName;
    }

    public static String getHaLockPath() {
        return BASE_PATH + DATA_HOSTS + SEPARATOR + DATA_HOST_LOCKS + SEPARATOR;
    }

    public static String getHaLockPath(String dhName) {
        return getHaLockPath() + dhName;
    }

}
