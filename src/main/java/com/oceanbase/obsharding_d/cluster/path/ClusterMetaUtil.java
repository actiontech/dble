/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.path;

import com.oceanbase.obsharding_d.cluster.values.*;

/**
 * @author dcy
 * Create Date: 2021-04-06
 */
public final class ClusterMetaUtil {
    private ClusterMetaUtil() {

    }

    public static PathMeta<RawJson> getConfShardingPath() {
        return PathMeta.of(ClusterPathUtil.getConfShardingPath(), RawJson.class);
    }

    public static PathMeta<RawJson> getUserConfPath() {
        return PathMeta.of(ClusterPathUtil.getUserConfPath(), RawJson.class);
    }

    public static PathMeta<RawJson> getDbConfPath() {
        return PathMeta.of(ClusterPathUtil.getDbConfPath(), RawJson.class);
    }


    public static PathMeta<RawJson> getHaStatusPath(String dbGroupName) {
        return PathMeta.of(ClusterPathUtil.getHaStatusPath(dbGroupName), RawJson.class);
    }


    public static PathMeta<HaInfo> getHaResponseChildPath(String dhName) {
        return PathMeta.of(ClusterPathUtil.getHaResponsePath(dhName), HaInfo.class);
    }

    public static PathMeta<HaInfo> getHaLockPath(String dhName) {
        return PathMeta.of(ClusterPathUtil.getHaLockPath(dhName), HaInfo.class);
    }


    public static PathMeta<RawJson> getSequencesCommonPath() {
        return PathMeta.of(ClusterPathUtil.getSequencesCommonPath(), RawJson.class);
    }


    public static PathMeta<ClusterTime> getSyncMetaLockPath() {
        return PathMeta.of(ClusterPathUtil.getSyncMetaLockPath(), ClusterTime.class);
    }

    public static PathMeta<Empty> getConfChangeLockPath() {
        return PathMeta.of(ClusterPathUtil.getConfChangeLockPath(), Empty.class);
    }


    public static PathMeta<ConfStatus> getConfStatusOperatorPath() {
        return PathMeta.of(ClusterPathUtil.getConfStatusOperatorPath(), ConfStatus.class);
    }


    public static PathMeta<OnlineType> getOnlinePath(String instanceName) {
        return PathMeta.of(ClusterPathUtil.getOnlinePath(instanceName), OnlineType.class);
    }

    public static PathMeta<Empty> getBinlogPauseLockPath() {
        return PathMeta.of(ClusterPathUtil.getBinlogPauseLockPath(), Empty.class);
    }


    public static PathMeta<Empty> getBinlogPauseStatusPath() {
        return PathMeta.of(ClusterPathUtil.getBinlogPauseStatusPath(), Empty.class);
    }

    public static PathMeta<FeedBackType> getBinlogPauseStatusSelfPath() {
        return PathMeta.of(ClusterPathUtil.getBinlogPauseStatusSelfPath(), FeedBackType.class);
    }


    public static PathMeta<DDLInfo> getDDLPath(String fullName, DDLInfo.NodeStatus ddlStatus) {
        return PathMeta.of(ClusterPathUtil.getDDLPath(fullName, ddlStatus), DDLInfo.class);
    }


    public static PathMeta<DDLInfo> getDDLLockPath(String fullName) {
        return PathMeta.of(ClusterPathUtil.getDDLLockPath(fullName), DDLInfo.class);
    }


    public static PathMeta<Empty> getPauseShardingNodeLockPath() {
        return PathMeta.of(ClusterPathUtil.getPauseShardingNodeLockPath(), Empty.class);
    }

    public static PathMeta<PauseInfo> getPauseResultNodePath() {
        return PathMeta.of(ClusterPathUtil.getPauseResultNodePath(), PauseInfo.class);
    }

    public static PathMeta<PauseInfo> getPauseResumePath() {
        return PathMeta.of(ClusterPathUtil.getPauseResumePath(), PauseInfo.class);
    }


    public static PathMeta<ViewType> getViewPath(String schemaName, String viewName) {
        return PathMeta.of(ClusterPathUtil.getViewPath(schemaName, viewName), ViewType.class);
    }


    public static PathMeta<ViewChangeType> getViewChangePath(String schemaName, String viewName) {
        return PathMeta.of(ClusterPathUtil.getViewChangePath(schemaName, viewName), ViewChangeType.class);
    }


    public static PathMeta<ViewChangeType> getViewLockPath(String schemaName, String viewName) {
        return PathMeta.of(ClusterPathUtil.getViewLockPath(schemaName, viewName), ViewChangeType.class);
    }
}
