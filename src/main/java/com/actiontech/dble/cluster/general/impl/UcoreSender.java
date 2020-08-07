/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.impl;

import com.actiontech.dble.alarm.UcoreGrpc;
import com.actiontech.dble.alarm.UcoreInterface;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.DebugUtil;
import com.actiontech.dble.util.PropertiesUtil;
import com.actiontech.dble.util.StringUtil;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterController.*;


/**
 * Created by szf on 2018/1/26.
 */
public final class UcoreSender extends AbstractConsulSender {


    private volatile UcoreGrpc.UcoreBlockingStub stub = null;
    private ConcurrentHashMap<String, Thread> lockMap = new ConcurrentHashMap<>();
    private List<String> ipList = new ArrayList<>();
    private static final String SOURCE_COMPONENT_TYPE = "dble";
    private String serverId = null;
    private String sourceComponentId = null;

    @Override
    public void initConInfo() {
        try {
            ipList.addAll(Arrays.asList(ClusterConfig.getInstance().getClusterIP().split(",")));
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
        Channel channel = ManagedChannelBuilder.forAddress(getIpList().get(0),
                ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
    }

    @Override
    public void initCluster() {
        serverId = SystemConfig.getInstance().getServerId();
        sourceComponentId = SystemConfig.getInstance().getInstanceName();
        try {
            ipList.addAll(Arrays.asList(ClusterConfig.getInstance().getClusterIP().split(",")));
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
        Channel channel = ManagedChannelBuilder.forAddress(getIpList().get(0),
                ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
        if (!skipSyncUcores()) {
            startUpdateNodes();
        }
        ClusterToXml.loadKVtoFile(this);
    }
    @Override
    public String lock(String path, String value) throws Exception {
        UcoreInterface.LockOnSessionInput input = UcoreInterface.LockOnSessionInput.newBuilder().setKey(path).setValue(value).setTTLSeconds(30).build();
        UcoreInterface.LockOnSessionOutput output;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).lockOnSession(input);
            if (!"".equals(output.getSessionId())) {
                final String session = output.getSessionId();
                Thread renewThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        String sessionId = session;
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                LOGGER.debug("renew lock of session  start:" + sessionId + " " + path);
                                if (StringUtil.isEmpty(ClusterHelper.getPathValue(path))) {
                                    log("renew lock of session  failure:" + sessionId + " " + path + ", the key is missing ", null);
                                    // alert
                                    Thread.currentThread().interrupt();
                                } else if (!renewLock(sessionId)) {
                                    log("renew lock of session  failure:" + sessionId + " " + path, null);
                                    // alert
                                } else {
                                    LOGGER.debug("renew lock of session  success:" + sessionId + " " + path);
                                }
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10000));
                            } catch (Exception e) {
                                log("renew lock of session  failure:" + sessionId + " " + path, e);
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5000));
                            }
                        }
                    }

                    private void log(String message, Exception e) {
                        if (!Thread.currentThread().isInterrupted()) {
                            if (e == null) {
                                LOGGER.warn(message);
                            } else {
                                LOGGER.warn(message, e);
                            }
                        }
                    }
                });
                lockMap.put(path, renewThread);
                renewThread.setName("UCORE_RENEW_" + path);
                renewThread.start();
            }
            return output.getSessionId();
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
        throw new IOException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
    }

    @Override
    public void unlockKey(String path, String sessionId) {
        UcoreInterface.UnlockOnSessionInput put = UcoreInterface.UnlockOnSessionInput.newBuilder().setKey(path).setSessionId(sessionId).build();
        try {
            Thread renewThread = lockMap.get(path);
            if (renewThread != null) {
                renewThread.interrupt();
            }
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).unlockOnSession(put);
        } catch (Exception e) {
            LOGGER.info(sessionId + " unlockKey " + path + " error ," + stub, e);
        }
    }

    @Override
    public void setKV(String path, String value) throws Exception {
        UcoreInterface.PutKvInput input = UcoreInterface.PutKvInput.newBuilder().setKey(path).setValue(value).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).putKv(input);
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
            throw new IOException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
        }
    }

    @Override
    public KvBean getKV(String path) {
        UcoreInterface.GetKvInput input = UcoreInterface.GetKvInput.newBuilder().setKey(path).build();
        UcoreInterface.GetKvOutput output = null;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKv(input);
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
                throw new RuntimeException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
            }
        }

        return new KvBean(path, output.getValue(), 0);
    }

    @Override
    public List<KvBean> getKVPath(String path) {
        if (!(path.charAt(path.length() - 1) == '/')) {
            path = path + "/";
        }
        List<KvBean> result = new ArrayList<KvBean>();
        UcoreInterface.GetKvTreeInput input = UcoreInterface.GetKvTreeInput.newBuilder().setKey(path).build();

        UcoreInterface.GetKvTreeOutput output = null;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore error ", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        if (output == null) {
            throw new RuntimeException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
        }
        for (int i = 0; i < output.getKeysCount(); i++) {
            KvBean bean = new KvBean(output.getKeys(i), output.getValues(i), output.getIndex());
            result.add(bean);
        }
        return result;
    }

    @Override
    public void cleanPath(String path) {
        if (!(path.charAt(path.length() - 1) == '/')) {
            path = path + "/";
        }
        UcoreInterface.DeleteKvTreeInput input = UcoreInterface.DeleteKvTreeInput.newBuilder().setKey(path).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKvTree(input);
        } catch (Exception e1) {
            boolean flag = false;
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
                throw new RuntimeException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
            }
        }
        cleanKV(path.substring(0, path.length() - 1));
    }

    public void cleanKV(String path) {
        UcoreInterface.DeleteKvInput input = UcoreInterface.DeleteKvInput.newBuilder().setKey(path).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKv(input);
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
            throw new RuntimeException("cleanKV failure for" + path);
        }
    }

    @Override
    public SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception {

        UcoreInterface.SubscribeKvPrefixInput input = UcoreInterface.SubscribeKvPrefixInput.newBuilder().
                setIndex(request.getIndex()).setDuration(request.getDuration()).setKeyPrefix(request.getPath()).build();
        try {
            UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
            return groupSubscribeResult(output);
        } catch (Exception e1) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS);
                    UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
                    return groupSubscribeResult(output);

                } catch (Exception e2) {
                    LOGGER.info("connect to ucore at " + ip + " failure", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        throw new IOException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
    }

    @Override
    public void alert(ClusterAlertBean alert) {
        UcoreInterface.AlertInput input = getInput(alert);
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alert(input);
        } catch (Exception e) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip, ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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

    @Override
    public boolean alertResolve(ClusterAlertBean alert) {
        UcoreInterface.AlertInput input = getInput(alert);
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alertResolve(input);
            return true;
        } catch (Exception e) {
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip, ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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

    private UcoreInterface.AlertInput getInput(ClusterAlertBean alert) {
        UcoreInterface.AlertInput.Builder builder = UcoreInterface.AlertInput.newBuilder().
                setCode(alert.getCode()).
                setDesc(alert.getDesc()).
                setLevel(alert.getLevel()).
                setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(sourceComponentId).
                setAlertComponentId(alert.getAlertComponentId()).
                setAlertComponentType(alert.getAlertComponentType()).
                setResolveTimestampUnix(alert.getResolveTimestampUnix()).
                setServerId(serverId).
                setTimestampUnix(alert.getTimestampUnix());
        if (alert.getLabels() != null) {
            builder.putAllLabels(alert.getLabels());
        }
        return builder.build();
    }


    public boolean renewLock(String sessionId) throws Exception {
        UcoreInterface.RenewSessionInput input = UcoreInterface.RenewSessionInput.newBuilder().setSessionId(sessionId).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).renewSession(input);
            return true;
        } catch (Exception e1) {
            LOGGER.info("connect to ucore renew error and will retry");
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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

    public SubscribeReturnBean groupSubscribeResult(UcoreInterface.SubscribeKvPrefixOutput output) {
        SubscribeReturnBean result = new SubscribeReturnBean();
        result.setIndex(output.getIndex());
        if (output.getKeysCount() > 0) {
            List<KvBean> kvList = new ArrayList<>();
            for (int i = 0; i < output.getKeysCount(); i++) {
                kvList.add(new KvBean(output.getKeys(i), output.getValues(i), 0));
            }
            result.setKvList(kvList);
        }
        return result;
    }

    public UcoreInterface.SubscribeNodesOutput subscribeNodes(UcoreInterface.SubscribeNodesInput subscribeNodesInput) throws IOException {
        try {
            return stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeNodes(subscribeNodesInput);
        } catch (Exception e) {
            //the first try failure ,try for all the other ucore ip
            for (String ip : getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
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
        throw new IOException(ERROR_MSG + "ips:" + ipList.toString() + ",port:" + ClusterConfig.getInstance().getClusterPort());
    }


    public void setIp(String ips) throws IOException {
        ClusterConfig.getInstance().setClusterIP(ips);
        Properties properties = ClusterConfig.getInstance().toProperties();
        PropertiesUtil.storeProperties(properties, CONFIG_FILE_NAME);
    }

    private void startUpdateNodes() {
        Thread nodes = new Thread(new Runnable() {
            @Override
            public void run() {
                long index = 0;
                for (; ; ) {
                    try {
                        UcoreInterface.SubscribeNodesInput subscribeNodesInput = UcoreInterface.SubscribeNodesInput.newBuilder().
                                setDuration(60).setIndex(index).build();
                        UcoreInterface.SubscribeNodesOutput output = subscribeNodes(subscribeNodesInput);
                        if (index != output.getIndex()) {
                            index = output.getIndex();
                            List<String> ips = new ArrayList<>();
                            for (int i = 0; i < output.getIpsList().size(); i++) {
                                ips.add(output.getIps(i));
                            }
                            LOGGER.info("old ucore ips :" + StringUtils.join(ipList, ','));
                            setIpList(ips);
                            String strIPs = StringUtils.join(ips, ',');
                            LOGGER.info("new ucore ips :" + strIPs);
                            setIp(strIPs);
                        }
                        firstReturnToCluster();
                    } catch (Exception e) {
                        LOGGER.warn("error in ucore nodes watch,try for another time", e);
                    }
                }
            }
        });
        nodes.setName("NODES_UCORE_LISTENER");
        nodes.start();
    }

    private void setIpList(List<String> ipList) {
        this.ipList = ipList;
    }

    private List<String> getIpList() {
        return ipList;
    }


    private boolean skipSyncUcores() {
        if (LOGGER.isDebugEnabled()) {
            String info = DebugUtil.getDebugInfo("skipSyncUcores.txt");
            return "skipSyncUcores".equals(info);
        }
        return false;
    }

}
