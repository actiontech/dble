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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterController.*;


/**
 * Created by szf on 2018/1/26.
 */
public final class UcoreSender extends AbstractConsulSender {


    /**
     * the single channel to the ucore cluster. the name resolver feeds it the ip list, the default pick_first policy
     * picks the node, reconnects with backoff and moves to the next ip by itself, so it is built once and never replaced
     */
    private volatile ManagedChannel channel = null;
    /**
     * pushes ucore node list changes to the channel
     */
    private volatile UcoreNameResolverProvider resolverProvider = null;
    private ConcurrentHashMap<String, Thread> lockMap = new ConcurrentHashMap<>();
    private List<String> ipList = new ArrayList<>();
    private static final String SOURCE_COMPONENT_TYPE = "dble";
    /**
     * methods that are safe to run twice, the only ones that get a retry policy
     */
    private static final List<String> IDEMPOTENT_METHODS = Arrays.asList("GetKv", "GetKvTree", "PutKv", "DeleteKv",
            "DeleteKvTree", "RenewSession", "UnlockOnSession");
    private String serverId = null;
    private String sourceComponentId = null;

    @Override
    public void initConInfo() {
        try {
            ipList.addAll(Arrays.asList(ClusterConfig.getInstance().getClusterIP().split(",")));
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
        initChannel();
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
        initChannel();
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
            output = call(GENERAL_GRPC_TIMEOUT, target -> target.lockOnSession(input));
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
            LOGGER.info("lock " + path + " error, ips:" + getIpList(), e1);
            throw e1;
        }
    }

    @Override
    public void unlockKey(String path, String sessionId) {
        UcoreInterface.UnlockOnSessionInput put = UcoreInterface.UnlockOnSessionInput.newBuilder().setKey(path).setSessionId(sessionId).build();
        try {
            Thread renewThread = lockMap.get(path);
            if (renewThread != null) {
                renewThread.interrupt();
            }
            call(GENERAL_GRPC_TIMEOUT, target -> target.unlockOnSession(put));
        } catch (Exception e) {
            LOGGER.info(sessionId + " unlockKey " + path + " error, ips:" + getIpList(), e);
        }
    }

    @Override
    public void setKV(String path, String value) throws Exception {
        UcoreInterface.PutKvInput input = UcoreInterface.PutKvInput.newBuilder().setKey(path).setValue(value).build();
        call(GENERAL_GRPC_TIMEOUT, target -> {
            target.putKv(input);
            return null;
        });
    }

    @Override
    public KvBean getKV(String path) {
        UcoreInterface.GetKvInput input = UcoreInterface.GetKvInput.newBuilder().setKey(path).build();
        UcoreInterface.GetKvOutput output;
        try {
            output = call(GENERAL_GRPC_TIMEOUT, target -> target.getKv(input));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
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

        UcoreInterface.GetKvTreeOutput output;
        try {
            output = call(GENERAL_GRPC_TIMEOUT, target -> target.getKvTree(input));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
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
            call(GENERAL_GRPC_TIMEOUT, target -> {
                target.deleteKvTree(input);
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        cleanKV(path.substring(0, path.length() - 1));
    }

    public void cleanKV(String path) {
        UcoreInterface.DeleteKvInput input = UcoreInterface.DeleteKvInput.newBuilder().setKey(path).build();
        try {
            call(GENERAL_GRPC_TIMEOUT, target -> {
                target.deleteKv(input);
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException("cleanKV failure for" + path, e);
        }
    }

    @Override
    public SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception {

        UcoreInterface.SubscribeKvPrefixInput input = UcoreInterface.SubscribeKvPrefixInput.newBuilder().
                setIndex(request.getIndex()).setDuration(request.getDuration()).setKeyPrefix(request.getPath()).build();
        UcoreInterface.SubscribeKvPrefixOutput output = call(GRPC_SUBTIMEOUT, target -> target.subscribeKvPrefix(input));
        return groupSubscribeResult(output);
    }

    @Override
    public void alert(ClusterAlertBean alert) {
        UcoreInterface.AlertInput input = getInput(alert);
        try {
            call(GENERAL_GRPC_TIMEOUT, target -> {
                target.alert(input);
                return null;
            });
        } catch (Exception e) {
            LOGGER.info("alert to ucore error, ips:" + getIpList(), e);
        }
    }

    @Override
    public boolean alertResolve(ClusterAlertBean alert) {
        UcoreInterface.AlertInput input = getInput(alert);
        try {
            call(GENERAL_GRPC_TIMEOUT, target -> {
                target.alertResolve(input);
                return null;
            });
            return true;
        } catch (Exception e) {
            LOGGER.info("alertResolve to ucore error, ips:" + getIpList(), e);
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
            call(GENERAL_GRPC_TIMEOUT, target -> {
                target.renewSession(input);
                return null;
            });
            return true;
        } catch (Exception e1) {
            LOGGER.info("connect to ucore renew error, ips:" + getIpList(), e1);
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
        return call(GRPC_SUBTIMEOUT, target -> target.subscribeNodes(subscribeNodesInput));
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
        UcoreNameResolverProvider provider = resolverProvider;
        if (provider != null) {
            provider.update(ipList);
        }
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

    /**
     * build the one channel to the ucore cluster, the way grpc-java's examples/nameresolve and examples/retrying do:
     * a custom scheme resolved by our NameResolverProvider, pick_first load balancing, and a service config that
     * enables retries for the idempotent calls. the channel connects lazily on the first rpc.
     */
    private synchronized void initChannel() {
        if (channel != null) {
            return;
        }
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(getIpList(), ClusterConfig.getInstance().getClusterPort());
        NameResolverRegistry.getDefaultRegistry().register(provider);
        resolverProvider = provider;
        channel = ManagedChannelBuilder.forTarget(UcoreNameResolverProvider.TARGET)
                .defaultLoadBalancingPolicy("pick_first")
                .defaultServiceConfig(retryServiceConfig())
                .enableRetry()
                .usePlaintext()
                .build();
    }

    /**
     * per-method retry policy, see https://grpc.io/docs/guides/retry/. a retry goes through the load balancer again,
     * so it can land on another ucore node. only calls that can safely run twice are listed: LockOnSession (not
     * idempotent), Alert/AlertResolve (a duplicate alert is noise) and the long polls (the caller loops anyway) are
     * left to the transparent retry, which only re-sends an rpc that never left the process.
     * numbers have to be Double, ManagedChannelBuilder#defaultServiceConfig does not accept Integer.
     */
    private static Map<String, Object> retryServiceConfig() {
        Map<String, Object> retryPolicy = new HashMap<>();
        retryPolicy.put("maxAttempts", 3.0);
        retryPolicy.put("initialBackoff", "0.2s");
        retryPolicy.put("maxBackoff", "2s");
        retryPolicy.put("backoffMultiplier", 2.0);
        retryPolicy.put("retryableStatusCodes", Collections.singletonList("UNAVAILABLE"));

        List<Object> names = new ArrayList<>();
        for (String method : IDEMPOTENT_METHODS) {
            Map<String, Object> name = new HashMap<>();
            name.put("service", UcoreGrpc.SERVICE_NAME);
            name.put("method", method);
            names.add(name);
        }
        Map<String, Object> methodConfig = new HashMap<>();
        methodConfig.put("name", names);
        methodConfig.put("retryPolicy", retryPolicy);

        Map<String, Object> serviceConfig = new HashMap<>();
        serviceConfig.put("methodConfig", Collections.singletonList(methodConfig));
        return serviceConfig;
    }

    /**
     * run one rpc on the shared channel. the stub is derived for every call, so the deadline is computed at call
     * time and the only thing shared between threads is the long-lived channel. failover between ucore nodes is the
     * channel's job (pick_first reconnects to the next ip with backoff), and the retry policy of the service config
     * re-sends the idempotent calls. wait-for-ready (https://grpc.io/docs/guides/wait-for-ready/) queues the rpc while
     * the channel is connecting instead of failing it at once, the deadline still bounds the wait.
     * a connection level failure is reported as IOException, any other status is ucore's answer on a healthy
     * connection and is rethrown unchanged.
     */
    private <T> T call(int timeoutSeconds, UcoreCall<T> rpc) throws IOException {
        initChannel();
        try {
            return rpc.invoke(UcoreGrpc.newBlockingStub(channel).withWaitForReady().withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS));
        } catch (RuntimeException e) {
            if (isConnectionFailure(e)) {
                throw new IOException(ERROR_MSG + "ips:" + getIpList() + ",port:" + ClusterConfig.getInstance().getClusterPort()
                        + " " + describe(e), e);
            }
            throw e;
        }
    }

    private static boolean isConnectionFailure(Throwable e) {
        Status status = statusOf(e);
        if (status == null) {
            return false;
        }
        return status.getCode() == Status.Code.UNAVAILABLE || status.getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    /**
     * grpc statuses reach us bare or wrapped, so look through the cause chain
     */
    private static Status statusOf(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof StatusRuntimeException) {
                return ((StatusRuntimeException) t).getStatus();
            }
            if (t instanceof StatusException) {
                return ((StatusException) t).getStatus();
            }
        }
        return null;
    }

    private static String describe(Throwable e) {
        Status status = statusOf(e);
        if (status == null) {
            return e.toString();
        }
        return status.getCode() + (status.getDescription() == null ? "" : " (" + status.getDescription() + ")");
    }

    /**
     * one rpc on a stub derived for the current call, kept last in the class (checkstyle InnerTypeLast)
     */
    private interface UcoreCall<T> {
        T invoke(UcoreGrpc.UcoreBlockingStub target);
    }

}
