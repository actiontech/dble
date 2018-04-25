package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterUcoreSender {

    private ClusterUcoreSender() {

    }

    private static UcoreGrpc.UcoreBlockingStub stub = null;

    {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_IP),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }

    public static void init() {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_IP),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }

    public static void sendDataToUcore(String key, String value) throws Exception {
        UcoreInterface.PutKvInput input = UcoreInterface.PutKvInput.newBuilder().setKey(key).setValue(value).build();
        if (stub == null) {
            init();
        }
        stub.putKv(input);
    }


    public static String lockKey(String key, String value) throws Exception {
        UcoreInterface.LockOnSessionInput input = UcoreInterface.LockOnSessionInput.newBuilder().setKey(key).setValue(value).setTTLSeconds(30).build();
        UcoreInterface.LockOnSessionOutput output = null;
        output = stub.lockOnSession(input);
        return output.getSessionId();
    }

    public static void renewLock(String sessionId) throws Exception {
        UcoreInterface.RenewSessionInput input = UcoreInterface.RenewSessionInput.newBuilder().setSessionId(sessionId).build();
        stub.renewSession(input);
    }

    public static void unlockKey(String key, String sessionId) {
        UcoreInterface.UnlockOnSessionInput put = UcoreInterface.UnlockOnSessionInput.newBuilder().setKey(key).setSessionId(sessionId).build();
        stub.unlockOnSession(put);
    }

    public static List<UKvBean> getKeyTree(String key) {
        if (!(key.charAt(key.length() - 1) == '/')) {
            key = key + "/";
        }
        List<UKvBean> result = new ArrayList<UKvBean>();
        UcoreInterface.GetKvTreeInput input = UcoreInterface.GetKvTreeInput.newBuilder().setKey(key).build();
        if (stub == null) {
            init();
        }
        UcoreInterface.GetKvTreeOutput output = stub.getKvTree(input);
        for (int i = 0; i < output.getKeysCount(); i++) {
            UKvBean bean = new UKvBean(output.getKeys(i), output.getValues(i), output.getIndex());
            result.add(bean);
        }
        return result;
    }

    public static UKvBean getKey(String key) {
        UcoreInterface.GetKvInput input = UcoreInterface.GetKvInput.newBuilder().setKey(key).build();
        if (stub == null) {
            init();
        }
        UcoreInterface.GetKvOutput output = stub.getKv(input);

        UKvBean bean = new UKvBean(key, output.getValue(), 0);
        return bean;
    }


    public static int getKeyTreeSize(String key) {
        UcoreInterface.GetKvTreeInput input = UcoreInterface.GetKvTreeInput.newBuilder().setKey(key).build();
        if (stub == null) {
            init();
        }
        UcoreInterface.GetKvTreeOutput output = stub.getKvTree(input);
        return output.getKeysCount();
    }

    public static void deleteKVTree(String key) {
        if (!(key.charAt(key.length() - 1) == '/')) {
            key = key + "/";
        }
        UcoreInterface.DeleteKvTreeInput input = UcoreInterface.DeleteKvTreeInput.newBuilder().setKey(key).build();
        if (stub == null) {
            init();
        }
        stub.deleteKvTree(input);
        UcoreInterface.DeleteKvInput inputSelf = UcoreInterface.DeleteKvInput.newBuilder().setKey(key.substring(0, key.length() - 1)).build();
        stub.deleteKv(inputSelf);
    }

    public static void deleteKV(String key) {
        UcoreInterface.DeleteKvInput input = UcoreInterface.DeleteKvInput.newBuilder().setKey(key).build();
        if (stub == null) {
            init();
        }
        stub.deleteKv(input);
    }
}
