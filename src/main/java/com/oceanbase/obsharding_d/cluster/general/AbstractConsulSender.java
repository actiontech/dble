/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general;

import com.oceanbase.obsharding_d.backend.mysql.view.FileSystemRepository;
import com.oceanbase.obsharding_d.backend.mysql.view.KVStoreRepository;
import com.oceanbase.obsharding_d.backend.mysql.view.Repository;
import com.oceanbase.obsharding_d.cluster.ClusterSender;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.general.bean.ClusterAlertBean;
import com.oceanbase.obsharding_d.cluster.general.bean.SubscribeRequest;
import com.oceanbase.obsharding_d.cluster.general.bean.SubscribeReturnBean;
import com.oceanbase.obsharding_d.cluster.general.kVtoXml.ClusterToXml;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.general.response.*;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.OnlineType;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.singleton.OnlineStatus;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by szf on 2019/3/11.
 */
public abstract class AbstractConsulSender implements ClusterSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsulSender.class);

    protected ConcurrentHashMap<String, Thread> lockMap = new ConcurrentHashMap<>();

    protected static final String ERROR_MSG = "ALL the url to cluster connect failure,";

    public abstract String getRenewThreadPrefix();

    /**
     * only init the connection preperties for the clusterSender
     * mainly used by shell to upload xml config
     * do not start any Thread in this function!
     */
    public abstract void initConInfo();

    /**
     * lock a path,so that other OBsharding_DServer in cluster can not hold the path in the same time
     * return sessionId of the lock,if the return String is not null or "" means lock success
     * the lock value is also import ,the lock also need to be regarded as a KV
     * <p>
     * NOTICE: the lock should only influences it self,the child path should be available to other OBsharding_DServer to writeDirectly
     *
     * @param path
     * @param value
     * @return
     * @throws Exception
     */
    public abstract String lock(String path, String value) throws Exception;


    /**
     * use the locked path and sessionId to unlock a path
     * and the KV of the lock should be removed
     *
     * @param key
     * @param sessionId
     */
    public abstract void unlockKey(String key, String sessionId);


    /**
     * alert something into cluster
     *
     * @param alert
     */
    public abstract void alert(ClusterAlertBean alert);

    /**
     * notify cluster some alert is resolved
     *
     * @param alert
     * @return
     */
    public abstract boolean alertResolve(ClusterAlertBean alert);

    public abstract SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception;


    @Override
    public void createSelfTempNode(String path, String value) throws Exception {
        String selfPath;
        if (path.endsWith(ClusterPathUtil.SEPARATOR)) {
            selfPath = path + SystemConfig.getInstance().getInstanceName();
        } else {
            selfPath = path + ClusterPathUtil.SEPARATOR + SystemConfig.getInstance().getInstanceName();
        }
        setKV(selfPath, value);

        LOGGER.info("writeDirectly self node for path:" + selfPath);
    }


    @Override
    public void writeConfToCluster() throws Exception {
        ClusterClearKeyListener ucoreListen = new ClusterClearKeyListener(this);


        new XmlDbLoader().registerPrefixForUcore(ucoreListen);
        new XmlShardingLoader().registerPrefixForUcore(ucoreListen);
        new XmlUserLoader().registerPrefixForUcore(ucoreListen);
        new SequencePropertiesLoader().registerPrefixForUcore(ucoreListen);

        ucoreListen.initAllNode();
        new DbGroupHaResponse().notifyCluster();
    }

    @Override
    public DistributeLock createDistributeLock(String path, String value) {
        return new ConsulDistributeLock(path, value, this);
    }

    @Override
    public DistributeLock createDistributeLock(String path, String value, int maxErrorCnt) {
        return new ConsulDistributeLock(path, value, maxErrorCnt, this);
    }


    @Override
    public Map<String, OnlineType> getOnlineMap() {
        return ClusterToXml.getOnlineMap();
    }

    @Override
    public void forceResumePause() throws Exception {
        ClusterXmlLoader loader = ClusterToXml.getListener().getResponse(ClusterPathUtil.getPauseResultNodePath());
        loader.notifyCluster();
    }

    /**
     * only be called when the OBsharding-D start with cluster disconnect
     * & OBsharding-D connect to cluster after a while
     * init some of the status from cluster
     *
     * @throws Exception
     */
    protected void firstReturnToCluster() throws Exception {
        if (ProxyMeta.getInstance().getTmManager() != null) {
            if (ProxyMeta.getInstance().getTmManager().getRepository() != null &&
                    ProxyMeta.getInstance().getTmManager().getRepository() instanceof FileSystemRepository) {
                LOGGER.warn("OBsharding-D first reconnect to ucore ,local view repository change to KVStoreRepository");
                Repository newViewRepository = new KVStoreRepository();
                ProxyMeta.getInstance().getTmManager().setRepository(newViewRepository);
                Map<String, Map<String, String>> viewCreateSqlMap = newViewRepository.getViewCreateSqlMap();
                ProxyMeta.getInstance().getTmManager().reloadViewMeta(viewCreateSqlMap);
                //init online status
                LOGGER.warn("OBsharding-D first reconnect to ucore ,online status rebuild");
            }
            OnlineStatus.getInstance().nodeListenerInitClusterOnline();
        }
    }

    public List<String> fetchRenewThread() {
        String onlineStr = ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName());
        List<String> renewThread = lockMap.values().stream().
                filter(c -> !c.getName().endsWith(onlineStr) && c.isAlive()).
                map(Thread::getName).collect(Collectors.toList());
        return renewThread;
    }

    public boolean killRenewThread(String path) {
        Thread renewThread = lockMap.get(path);
        if (renewThread != null) {
            if (renewThread.isAlive() && !renewThread.isInterrupted()) {
                renewThread.interrupt();
                LOGGER.info("manual kill cluster renew thread: [{}]", renewThread.getName());
            } else {
                if (!renewThread.isAlive()) {
                    LOGGER.info("try manual kill cluster renew thread: [{}], but it already terminated", renewThread.getName());
                } else {
                    LOGGER.info("try manual kill cluster renew thread: [{}], but it already killed", renewThread.getName());
                }
            }
            return true;
        }
        return false;
    }
}
