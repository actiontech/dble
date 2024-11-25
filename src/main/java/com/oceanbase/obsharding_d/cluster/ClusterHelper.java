/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.general.bean.KvBean;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.path.PathMeta;
import com.oceanbase.obsharding_d.cluster.values.*;
import com.oceanbase.obsharding_d.services.manager.response.ReloadConfig;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterHelper {
    private final ClusterOperation operation;

    private ClusterHelper(ClusterOperation operation) {
        this.operation = operation;
    }

    public static ClusterHelper getInstance(ClusterOperation operation) {
        return new ClusterHelper(operation);
    }


    @Nonnull
    public <T> Optional<ClusterValue<T>> getPathValue(PathMeta<T> path) throws Exception {
        if (ClusterGeneralConfig.getInstance().getClusterSender() == null) {
            return Optional.empty();
        }
        final KvBean data = ClusterGeneralConfig.getInstance().getClusterSender().getKV(path.getPath());
        if (isNotEmptyValue(data)) {
            return Optional.ofNullable(ClusterValue.readFromJson(data.getValue(), path.getTClass()));
        }
        return Optional.empty();
    }

    /**
     * ignore the node with empty value.
     */
    private static boolean isNotEmptyValue(KvBean kvBean) {
        return kvBean != null && !Strings.isEmpty(kvBean.getValue());
    }

    public int getChildrenSize(String path) throws Exception {
        return ClusterLogic.forCommon(operation).getKVBeanOfChildPath(ChildPathMeta.of(path, Empty.class)).size();
    }


    public <T> List<ClusterEntry<T>> getKVPath(ChildPathMeta<T> meta) throws Exception {
        if (null == ClusterGeneralConfig.getInstance().getClusterSender()) {
            return Collections.EMPTY_LIST;
        } else {
            final List<KvBean> kvPath = ClusterGeneralConfig.getInstance().getClusterSender().getKVPath(meta.getPath());
            List<ClusterEntry<T>> list = new ArrayList<>();
            for (KvBean kvBean : kvPath) {
                if (isNotEmptyValue(kvBean)) {
                    list.add(new ClusterEntry<T>(kvBean.getKey(), ClusterValue.readFromJson(kvBean.getValue(), meta.getChildClass())));
                }
            }
            return list;
        }
    }

    public <T> void setKV(PathMeta<T> pathMeta, @Nonnull T value) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().setKV(pathMeta.getPath(), constructAndSerializeValue(value));
    }

    public DistributeLock createDistributeLock(PathMeta<Empty> path) {
        return createDistributeLock(path, new Empty());
    }

    public <T> DistributeLock createDistributeLock(PathMeta<T> path, @Nonnull T value) {
        return ClusterGeneralConfig.getInstance().getClusterSender().createDistributeLock(path.getPath(), constructAndSerializeValue(value));
    }

    public <T> DistributeLock createDistributeLock(PathMeta<T> path, @Nonnull T value, int maxErrorCnt) {
        return ClusterGeneralConfig.getInstance().getClusterSender().createDistributeLock(path.getPath(), constructAndSerializeValue(value), maxErrorCnt);
    }

    public <T> void createSelfTempNode(String path, @Nonnull FeedBackType value) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().createSelfTempNode(path, constructAndSerializeValue(value));
    }

    public static void cleanKV(String path) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanKV(path);
    }


    public static void cleanKV(PathMeta<?> pathMeta) throws Exception {
        cleanKV(pathMeta.getPath());
    }


    public static void cleanPath(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanPath(path);
    }

    public static void cleanPath(PathMeta<?> pathMeta) throws Exception {
        cleanPath(pathMeta.getPath());
    }


    public static Map<String, OnlineType> getOnlineMap() {
        return ClusterGeneralConfig.getInstance().getClusterSender().getOnlineMap();
    }

    public static void writeConfToCluster(ReloadConfig.ReloadResult reloadResult) throws Exception {
        ClusterLogic.forConfig().syncSequenceJsonToCluster();
        ClusterLogic.forConfig().syncDbJsonToCluster();
        ClusterLogic.forConfig().syncShardingJsonToCluster();
        ClusterLogic.forConfig().syncUseJsonToCluster();
        ClusterLogic.forHA().syncDbGroupStatusToCluster(reloadResult);
    }

    @Nullable
    public static Boolean isExist(String path) throws Exception {
        if (ClusterGeneralConfig.getInstance().getClusterSender() == null) {
            return null;
        }
        final KvBean data = ClusterGeneralConfig.getInstance().getClusterSender().getKV(path);
        if (isNotEmptyValue(data)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }


    private <T> String constructAndSerializeValue(@Nonnull T value) {
        return ClusterValue.constructForWrite(value, operation.getApiVersion()).toJson();
    }


    public static void forceResumePause() throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().forceResumePause();
    }

}
