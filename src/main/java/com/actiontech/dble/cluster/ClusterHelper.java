package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.general.bean.KvBean;

import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterHelper {
    private ClusterHelper() {

    }

    public static void setKV(String path, String value) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().setKV(path, value);
    }


    public static void createSelfTempNode(String path, String value) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().createSelfTempNode(path, value);
    }

    public static KvBean getKV(String path) throws Exception {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKV(path);
    }

    public static void cleanKV(String path) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanKV(path);
    }

    public static int getChildrenSize(String path) throws Exception {
        return ClusterLogic.getKVBeanOfChildPath(path).size();
    }

    public static void cleanPath(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanPath(path);
    }

    public static DistributeLock createDistributeLock(String path, String value) {
        return ClusterGeneralConfig.getInstance().getClusterSender().createDistributeLock(path, value);
    }


    public static DistributeLock createDistributeLock(String path, String value, int maxErrorCnt) {
        return ClusterGeneralConfig.getInstance().getClusterSender().createDistributeLock(path, value, maxErrorCnt);
    }

    public static Map<String, String> getOnlineMap() {
        return ClusterGeneralConfig.getInstance().getClusterSender().getOnlineMap();
    }

    public static void writeConfToCluster() throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().writeConfToCluster();
    }

    public static String getPathValue(String path) throws Exception {
        KvBean kv = getKV(path);
        if (kv == null) {
            return null;
        } else {
            return kv.getValue();
        }
    }


    public static String getPathKey(String path) throws Exception {
        KvBean kv = getKV(path);
        if (kv == null) {
            return null;
        } else {
            return kv.getKey();
        }
    }

    public static List<KvBean> getKVPath(String path) throws Exception {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKVPath(path);
    }

    public static void forceResumePause() throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().forceResumePause();
    }

}
