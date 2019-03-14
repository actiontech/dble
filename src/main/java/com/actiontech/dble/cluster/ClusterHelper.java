package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.bean.SubscribeRequest;
import com.actiontech.dble.cluster.bean.SubscribeReturnBean;

import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterHelper {

    private ClusterHelper() {

    }

    public static String lock(String path, String value) throws Exception {
        return ClusterGeneralConfig.getInstance().getClusterSender().lock(path, value);
    }

    public static void unlockKey(String path, String sessionId) {
        ClusterGeneralConfig.getInstance().getClusterSender().unlockKey(path, sessionId);
    }

    public static void setKV(String path, String value) throws Exception {
        ClusterGeneralConfig.getInstance().getClusterSender().setKV(path, value);
    }

    public static KvBean getKV(String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKV(path);
    }

    public static void cleanKV(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanKV(path);
    }

    public static List<KvBean> getKVPath(String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().getKVPath(path);
    }

    public static void cleanPath(String path) {
        ClusterGeneralConfig.getInstance().getClusterSender().cleanPath(path);
    }

    public static boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        return ClusterGeneralConfig.getInstance().getClusterSender().checkResponseForOneTime(checkString, path, expectedMap, errorMsg);
    }

    public static String waitingForAllTheNode(String checkString, String path) {
        return ClusterGeneralConfig.getInstance().getClusterSender().waitingForAllTheNode(checkString, path);
    }

    public static void alert(ClusterAlertBean alert) {
        ClusterGeneralConfig.getInstance().getClusterSender().alert(alert);
    }

    public static boolean alertResolve(ClusterAlertBean alert) {
        return ClusterGeneralConfig.getInstance().getClusterSender().alertResolve(alert);
    }

    public static SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception {
        return ClusterGeneralConfig.getInstance().getClusterSender().subscribeKvPrefix(request);
    }


}
