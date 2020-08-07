package com.actiontech.dble.cluster.general.impl.ushard;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.StringUtil;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterController.GENERAL_GRPC_TIMEOUT;
import static com.actiontech.dble.cluster.ClusterController.GRPC_SUBTIMEOUT;

/**
 * Created by szf on 2019/3/13.
 */
public class UshardSender extends AbstractConsulSender {

    private volatile DbleClusterGrpc.DbleClusterBlockingStub stub = null;
    private ConcurrentHashMap<String, Thread> lockMap = new ConcurrentHashMap<>();
    private final String sourceComponentType = "dble";
    private String serverId = null;
    private String sourceComponentId = null;

    @Override
    public void initConInfo() {
        Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1",
                ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
        stub = DbleClusterGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
    }

    @Override
    public void initCluster() {
        serverId = SystemConfig.getInstance().getServerId();
        sourceComponentId = SystemConfig.getInstance().getInstanceName();
        Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1",
                ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
        stub = DbleClusterGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
        startUpdateNodes();
        ClusterToXml.loadKVtoFile(this);
    }

    @Override
    public String lock(String path, String value) throws Exception {
        UshardInterface.LockOnSessionInput input = UshardInterface.LockOnSessionInput.newBuilder().setKey(path).setValue(value).setTTLSeconds(30).build();
        UshardInterface.LockOnSessionOutput output;

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
            throw new IOException("ushard connect failure", e1);
        }
    }

    @Override
    public void unlockKey(String path, String sessionId) {
        UshardInterface.UnlockOnSessionInput put = UshardInterface.UnlockOnSessionInput.newBuilder().setKey(path).setSessionId(sessionId).build();
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
        UshardInterface.PutKvInput input = UshardInterface.PutKvInput.newBuilder().setKey(path).setValue(value).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).putKv(input);
        } catch (Exception e1) {
            throw new IOException(ERROR_MSG);
        }
    }

    @Override
    public KvBean getKV(String path) {
        UshardInterface.GetKvInput input = UshardInterface.GetKvInput.newBuilder().setKey(path).build();
        UshardInterface.GetKvOutput output;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKv(input);
        } catch (Exception e1) {
            throw new RuntimeException(ERROR_MSG);
        }

        return new KvBean(path, output.getValue(), 0);
    }

    @Override
    public List<KvBean> getKVPath(String path) {
        if (!(path.charAt(path.length() - 1) == '/')) {
            path = path + "/";
        }
        List<KvBean> result = new ArrayList<>();
        UshardInterface.GetKvTreeInput input = UshardInterface.GetKvTreeInput.newBuilder().setKey(path).build();

        UshardInterface.GetKvTreeOutput output;

        try {
            output = stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).getKvTree(input);
        } catch (Exception e1) {
            throw new RuntimeException(ERROR_MSG);
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
        UshardInterface.DeleteKvTreeInput input = UshardInterface.DeleteKvTreeInput.newBuilder().setKey(path).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKvTree(input);
        } catch (Exception e1) {
            throw new RuntimeException(ERROR_MSG);
        }
        cleanKV(path.substring(0, path.length() - 1));
    }

    @Override
    public void cleanKV(String path) {
        UshardInterface.DeleteKvInput input = UshardInterface.DeleteKvInput.newBuilder().setKey(path).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).deleteKv(input);
        } catch (Exception e1) {
            throw new RuntimeException("cleanKV failure for" + path);
        }
    }

    @Override
    public SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception {
        UshardInterface.SubscribeKvPrefixInput input = UshardInterface.SubscribeKvPrefixInput.newBuilder().
                setIndex(request.getIndex()).setDuration(request.getDuration()).setKeyPrefix(request.getPath()).build();
        try {
            UshardInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
            return groupSubscribeResult(output);
        } catch (Exception e1) {
            throw new IOException("ushard connect failure");
        }

    }

    @Override
    public void alert(ClusterAlertBean alert) {
        UshardInterface.AlertInput input = getInput(alert);
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alert(input);
        } catch (Exception e) {
            LOGGER.info("alert to ushard error ", e);
        }
    }

    @Override
    public boolean alertResolve(ClusterAlertBean alert) {
        UshardInterface.AlertInput input = getInput(alert);
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).alertResolve(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    private boolean renewLock(String sessionId) throws Exception {
        UshardInterface.RenewSessionInput input = UshardInterface.RenewSessionInput.newBuilder().setSessionId(sessionId).build();
        try {
            stub.withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS).renewSession(input);
            return true;
        } catch (Exception e1) {
            LOGGER.info("connect to ushard renew error and will retry");
            return false;
        }
    }

    private SubscribeReturnBean groupSubscribeResult(UshardInterface.SubscribeKvPrefixOutput output) {
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


    private UshardInterface.AlertInput getInput(ClusterAlertBean alert) {
        UshardInterface.AlertInput.Builder builder = UshardInterface.AlertInput.newBuilder().
                setCode(alert.getCode()).
                setDesc(alert.getDesc()).
                setLevel(alert.getLevel()).
                setSourceComponentType(sourceComponentType).
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


    private void startUpdateNodes() {
        Thread nodes = new Thread(new Runnable() {
            @Override
            public void run() {
                for (; ; ) {
                    try {
                        UshardInterface.SubscribeKvPrefixInput input = UshardInterface.SubscribeKvPrefixInput.newBuilder().
                                setIndex(0).setDuration(60).setKeyPrefix(ClusterPathUtil.BASE_PATH).build();
                        stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
                        firstReturnToCluster();
                        return;
                    } catch (Exception e) {
                        LOGGER.warn("error in ucore nodes watch,try for another time", e);
                        Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1",
                                ClusterConfig.getInstance().getClusterPort()).usePlaintext(true).build();
                        stub = DbleClusterGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                    }
                }
            }
        });
        nodes.setName("USHARD_RECONNECT_LISTENER");
        nodes.start();
    }
}
