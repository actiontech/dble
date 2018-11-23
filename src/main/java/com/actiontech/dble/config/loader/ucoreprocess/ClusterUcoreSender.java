package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.alarm.UcoreGrpc;
import com.actiontech.dble.alarm.UcoreInterface;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.KVtoXml.UcoreToXml;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterController.GENERAL_GRPC_TIMEOUT;
import static com.actiontech.dble.cluster.ClusterController.GRPC_SUBTIMEOUT;

/**
 * Created by szf on 2018/1/26.
 */
public final class ClusterUcoreSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreXmlLoader.class);

    private ClusterUcoreSender() {

    }

    private static volatile UcoreGrpc.UcoreBlockingStub stub = null;

    {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
    }


    public static void sendDataToUcore(String key, String value) throws Exception {
        UcoreInterface.PutKvInput input = UcoreInterface.PutKvInput.newBuilder().setKey(key).setValue(value).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).putKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).putKv(input);
                    return;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
            throw new IOException("ALL the ucore connect failure");
        }
    }


    public static String lockKey(String key, String value) throws Exception {
        UcoreInterface.LockOnSessionInput input = UcoreInterface.LockOnSessionInput.newBuilder().setKey(key).setValue(value).setTTLSeconds(30).build();
        UcoreInterface.LockOnSessionOutput output = null;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).lockOnSession(input);
            return output.getSessionId();
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).lockOnSession(input);
                    return output.getSessionId();
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
    }

    static boolean renewLock(String sessionId) throws Exception {
        UcoreInterface.RenewSessionInput input = UcoreInterface.RenewSessionInput.newBuilder().setSessionId(sessionId).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).renewSession(input);
            return true;
        } catch (Exception e1) {
            LOGGER.info("connect to ucore renew error and will retry");
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).renewSession(input);
                    return true;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore renew error " + stub, e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
            return false;
        }
    }

    static void unlockKey(String key, String sessionId) {
        UcoreInterface.UnlockOnSessionInput put = UcoreInterface.UnlockOnSessionInput.newBuilder().setKey(key).setSessionId(sessionId).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).unlockOnSession(put);
        } catch (Exception e) {
            LOGGER.info(sessionId + " unlockKey " + key + " error ," + stub, e);
        }
    }

    public static List<UKvBean> getKeyTree(String key) {
        if (!(key.charAt(key.length() - 1) == '/')) {
            key = key + "/";
        }
        List<UKvBean> result = new ArrayList<UKvBean>();
        UcoreInterface.GetKvTreeInput input = UcoreInterface.GetKvTreeInput.newBuilder().setKey(key).build();

        UcoreInterface.GetKvTreeOutput output = null;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
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
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKv(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
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
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
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
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKvTree(input);
        } catch (Exception e1) {
            boolean flag = false;
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKvTree(input);
                    flag = true;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
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
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKv(input);
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKv(input);
                    return;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
            throw new RuntimeException("ALL the ucore connect failure");
        }
    }


    public static UcoreInterface.SubscribeKvPrefixOutput subscribeKvPrefix(UcoreInterface.SubscribeKvPrefixInput input) throws IOException {
        try {
            UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
            return output;
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS);
                    UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
                    return output;

                } catch (Exception e2) {
                    LOGGER.info("connect to ucore at " + ip + " failure", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
    }


    public static UcoreInterface.SubscribeNodesOutput subscribeNodes(UcoreInterface.SubscribeNodesInput subscribeNodesInput) throws IOException {
        try {
            return stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeNodes(subscribeNodesInput);
        } catch (Exception e) {
            //the first try failure ,try for all the other ucore ip
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS);
                    return stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeNodes(subscribeNodesInput);
                } catch (Exception e2) {
                    LOGGER.info("try connection IP " + ip + " failure ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
    }


    public static void alert(UcoreInterface.AlertInput input) {
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alert(input);
        } catch (Exception e) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip, Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alert(input);
                    return;
                } catch (Exception e2) {
                    LOGGER.info("alert to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
    }

    public static boolean alertResolve(UcoreInterface.AlertInput input) {
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alertResolve(input);
            return true;
        } catch (Exception e) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip, Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alertResolve(input);
                    return true;
                } catch (Exception e2) {
                    LOGGER.info("alertResolve to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                    return false;
                }
            }
            return false;
        }
    }

    public static String waitingForAllTheNode(String checkString, String path) {
        Map<String, String> expectedMap = UcoreToXml.getOnlineMap();
        StringBuffer errorMsg = new StringBuffer();
        for (; ; ) {
            errorMsg.setLength(0);
            if (checkResponseForOneTime(checkString, path, expectedMap, errorMsg)) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
        return errorMsg.length() <= 0 ? null : errorMsg.toString();
    }


    public static boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        Map<String, String> currentMap = UcoreToXml.getOnlineMap();
        checkOnline(expectedMap, currentMap);
        List<UKvBean> responseList = ClusterUcoreSender.getKeyTree(path);
        boolean flag = false;
        for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
            flag = false;
            for (UKvBean uKvBean : responseList) {
                String responseNode = last(uKvBean.getKey().split("/"));
                if (last(entry.getKey().split("/")).
                        equals(responseNode)) {
                    if (checkString != null) {
                        if (!checkString.equals(uKvBean.getValue())) {
                            if (errorMsg != null) {
                                errorMsg.append(responseNode).append(":").append(uKvBean.getValue()).append(";");
                            }
                        }
                    }
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                break;
            }
        }

        return flag;
    }


    public static void checkOnline(Map<String, String> expectedMap, Map<String, String> currentMap) {
        Iterator<Map.Entry<String, String>> iterator = expectedMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (!currentMap.containsKey(entry.getKey()) ||
                    (currentMap.containsKey(entry.getKey()) && !currentMap.get(entry.getKey()).equals(entry.getValue()))) {
                iterator.remove();
            }
        }

        for (Map.Entry<String, String> entry : currentMap.entrySet()) {
            if (!expectedMap.containsKey(entry.getKey())) {
                LOGGER.warn("NODE " + entry.getKey() + " IS NOT EXPECTED TO BE ONLINE,PLEASE CHECK IT ");
            }
        }
    }

    public static <T> T last(T[] array) {
        return array[array.length - 1];
    }


}
