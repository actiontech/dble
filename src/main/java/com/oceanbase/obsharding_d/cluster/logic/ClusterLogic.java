/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

public final class ClusterLogic {

    private static final BinlogClusterLogic BINLOG_CLUSTER_LOGIC = new BinlogClusterLogic();
    private static final ConfigClusterLogic CONFIG_CLUSTER_LOGIC = new ConfigClusterLogic();
    private static final DDLClusterLogic DDL_CLUSTER_LOGIC = new DDLClusterLogic();
    private static final HAClusterLogic HA_CLUSTER_LOGIC = new HAClusterLogic();
    private static final MetaClusterLogic META_CLUSTER_LOGIC = new MetaClusterLogic();
    private static final PauseResumeClusterLogic PAUSE_RESUME_CLUSTER_LOGIC = new PauseResumeClusterLogic();
    private static final ViewClusterLogic VIEW_CLUSTER_LOGIC = new ViewClusterLogic();
    private static final OnlineClusterLogic ONLINE_CLUSTER_LOGIC = new OnlineClusterLogic();
    private static final GeneralClusterLogic GENERAL_CLUSTER_LOGIC = new GeneralClusterLogic();

    private ClusterLogic() {
    }

    public static GeneralClusterLogic forGeneral() {
        return GENERAL_CLUSTER_LOGIC;
    }

    public static CommonClusterLogic forCommon(ClusterOperation type) {
        return new CommonClusterLogic(type);
    }

    public static BinlogClusterLogic forBinlog() {
        return BINLOG_CLUSTER_LOGIC;
    }

    public static ConfigClusterLogic forConfig() {
        return CONFIG_CLUSTER_LOGIC;
    }

    public static DDLClusterLogic forDDL() {
        return DDL_CLUSTER_LOGIC;
    }

    public static HAClusterLogic forHA() {
        return HA_CLUSTER_LOGIC;
    }

    public static MetaClusterLogic forMeta() {
        return META_CLUSTER_LOGIC;
    }

    public static OnlineClusterLogic forOnline() {
        return ONLINE_CLUSTER_LOGIC;
    }

    public static PauseResumeClusterLogic forPauseResume() {
        return PAUSE_RESUME_CLUSTER_LOGIC;
    }

    public static ViewClusterLogic forView() {
        return VIEW_CLUSTER_LOGIC;
    }

}
