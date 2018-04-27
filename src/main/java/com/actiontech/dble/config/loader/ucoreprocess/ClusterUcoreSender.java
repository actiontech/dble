package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterUcoreSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreXmlLoader.class);

    private ClusterUcoreSender() {

    }

    private static UcoreGrpc.UcoreBlockingStub stub = null;

    {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }


    public static void sendDataToUcore(String key, String value) throws Exception {
        UcoreInterface.PutKvInput input = UcoreInterface.PutKvInput.newBuilder().setKey(key).setValue(value).build();
        try {
            stub.putKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    stub.putKv(input);
                    return;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            throw new IOException("ALL the ucore connect failure");
        }
    }


    public static String lockKey(String key, String value) throws Exception {
        UcoreInterface.LockOnSessionInput input = UcoreInterface.LockOnSessionInput.newBuilder().setKey(key).setValue(value).setTTLSeconds(30).build();
        UcoreInterface.LockOnSessionOutput output = null;

        try {
            output = stub.lockOnSession(input);
            return output.getSessionId();
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    output = stub.lockOnSession(input);
                    return output.getSessionId();
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
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

        UcoreInterface.GetKvTreeOutput output = null;

        try {
            output = stub.getKvTree(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    output = stub.getKvTree(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            if (output == null) {
                throw new RuntimeException("ALL the ucore connect failure");
            }
        }

        for (int i = 0; i < output.getKeysCount(); i++) {
            UKvBean bean = new UKvBean(output.getKeys(i), output.getValues(i), output.getIndex());
            result.add(bean);
        }
        return result;
    }

    public static UKvBean getKey(String key) {
        UcoreInterface.GetKvInput input = UcoreInterface.GetKvInput.newBuilder().setKey(key).build();
        UcoreInterface.GetKvOutput output = null;

        try {
            output = stub.getKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    output = stub.getKv(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            if (output == null) {
                throw new RuntimeException("ALL the ucore connect failure");
            }
        }

        UKvBean bean = new UKvBean(key, output.getValue(), 0);
        return bean;
    }


    public static int getKeyTreeSize(String key) {
        UcoreInterface.GetKvTreeInput input = UcoreInterface.GetKvTreeInput.newBuilder().setKey(key).build();
        UcoreInterface.GetKvTreeOutput output = null;

        try {
            output = stub.getKvTree(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    output = stub.getKvTree(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            if (output == null) {
                throw new RuntimeException("ALL the ucore connect failure");
            }
        }
        return output.getKeysCount();
    }

    public static void deleteKVTree(String key) {
        if (!(key.charAt(key.length() - 1) == '/')) {
            key = key + "/";
        }
        UcoreInterface.DeleteKvTreeInput input = UcoreInterface.DeleteKvTreeInput.newBuilder().setKey(key).build();
        try {
            stub.deleteKvTree(input);
        } catch (Exception e1) {
            boolean flag = false;
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    stub.deleteKvTree(input);
                    flag = true;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            if (!flag) {
                throw new RuntimeException("ALL the ucore connect failure");
            }
        }
        deleteKV(key.substring(0, key.length() - 1));
    }

    public static void deleteKV(String key) {
        UcoreInterface.DeleteKvInput input = UcoreInterface.DeleteKvInput.newBuilder().setKey(key).build();
        try {
            stub.deleteKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    stub.deleteKv(input);
                    return;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ");
                }
            }
            throw new RuntimeException("ALL the ucore connect failure");
        }
    }
}
