/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.path;

import com.oceanbase.obsharding_d.cluster.values.*;

/**
 * @author dcy
 * Create Date: 2021-04-06
 */
public final class ClusterChildMetaUtil {
    private ClusterChildMetaUtil() {

    }

    public static ChildPathMeta<RawJson> getConfShardingPath() {
        return ChildPathMeta.of(ClusterPathUtil.getConfShardingPath(), RawJson.class);
    }

    public static ChildPathMeta<RawJson> getUserConfPath() {
        return ChildPathMeta.of(ClusterPathUtil.getUserConfPath(), RawJson.class);
    }

    public static ChildPathMeta<RawJson> getDbConfPath() {
        return ChildPathMeta.of(ClusterPathUtil.getDbConfPath(), RawJson.class);
    }

    public static ChildPathMeta<RawJson> getSequencesCommonPath() {
        return ChildPathMeta.of(ClusterPathUtil.getSequencesCommonPath(), RawJson.class);
    }


    public static ChildPathMeta<ConfStatus> getConfStatusOperatorPath() {
        return ChildPathMeta.of(ClusterPathUtil.getConfStatusOperatorPath(), ConfStatus.class);
    }

    public static ChildPathMeta<Empty> getHaBasePath() {
        return ChildPathMeta.of(ClusterPathUtil.getHaBasePath(), Empty.class);
    }

    public static ChildPathMeta<RawJson> getHaStatusPath() {
        return ChildPathMeta.of(ClusterPathUtil.getHaStatusPath(), RawJson.class);
    }

    public static ChildPathMeta<HaInfo> getHaResponseChildPath() {
        return ChildPathMeta.of(ClusterPathUtil.getHaResponsePath(), HaInfo.class);
    }

    public static ChildPathMeta<ConfStatus> getConfStatusPath() {
        return ChildPathMeta.of(ClusterPathUtil.getConfStatusPath(), ConfStatus.class);
    }


    public static ChildPathMeta<OnlineType> getOnlinePath() {
        return ChildPathMeta.of(ClusterPathUtil.getOnlinePath(), OnlineType.class);
    }

    public static ChildPathMeta<Empty> getBinlogPausePath() {
        return ChildPathMeta.of(ClusterPathUtil.getBinlogPausePath(), Empty.class);
    }

    public static ChildPathMeta<DDLInfo> getDDLPath() {
        return ChildPathMeta.of(ClusterPathUtil.getDDLPath(), DDLInfo.class);
    }


    public static ChildPathMeta<ViewChangeType> getViewChangePath() {
        return ChildPathMeta.of(ClusterPathUtil.getViewChangePath(), ViewChangeType.class);
    }


    public static ChildPathMeta<PauseInfo> getPauseShardingNodePath() {
        return ChildPathMeta.of(ClusterPathUtil.getPauseShardingNodePath(), PauseInfo.class);
    }


    public static ChildPathMeta<FeedBackType> getPauseResultNodePath() {
        return ChildPathMeta.of(ClusterPathUtil.getPauseResultNodePath(), FeedBackType.class);
    }


    public static ChildPathMeta<FeedBackType> getHaResponsePath(String fullName) {
        return ChildPathMeta.of(ClusterPathUtil.getHaResponsePath(fullName), FeedBackType.class);
    }
}
