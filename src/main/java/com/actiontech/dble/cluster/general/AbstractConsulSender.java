/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general;

import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.KVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.ClusterSender;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.general.response.*;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.singleton.ProxyMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by szf on 2019/3/11.
 */
public abstract class AbstractConsulSender implements ClusterSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsulSender.class);

    protected static final String ERROR_MSG = "ALL the url to cluster connect failure,";

    /**
     * only init the connection preperties for the clusterSender
     * mainly used by shell to upload xml config
     * do not start any Thread in this function!
     */
    public abstract void initConInfo();

    /**
     * lock a path,so that other DbleServer in cluster can not hold the path in the same time
     * return sessionId of the lock,if the return String is not null or "" means lock success
     * the lock value is also import ,the lock also need to be regarded as a KV
     * <p>
     * NOTICE: the lock should only influences it self,the child path should be available to other DbleServer to writeDirectly
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

        XmlProcessBase xmlProcess = new XmlProcessBase();

        new XmlDbLoader(xmlProcess, ucoreListen);
        new XmlShardingLoader(xmlProcess, ucoreListen);
        new XmlUserLoader(xmlProcess, ucoreListen);
        new SequencePropertiesLoader(ucoreListen);
        xmlProcess.initJaxbClass();
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
    public Map<String, String> getOnlineMap() {
        return ClusterToXml.getOnlineMap();
    }

    @Override
    public void forceResumePause() throws Exception {
        ClusterXmlLoader loader = ClusterToXml.getListener().getResponse(ClusterPathUtil.getPauseResultNodePath());
        loader.notifyCluster();
    }

    /**
     * only be called when the dble start with cluster disconnect
     * & dble connect to cluster after a while
     * init some of the status from cluster
     *
     * @throws Exception
     */
    protected void firstReturnToCluster() throws Exception {
        if (ProxyMeta.getInstance().getTmManager() != null) {
            if (ProxyMeta.getInstance().getTmManager().getRepository() != null &&
                    ProxyMeta.getInstance().getTmManager().getRepository() instanceof FileSystemRepository) {
                LOGGER.warn("Dble first reconnect to ucore ,local view repository change to KVStoreRepository");
                Repository newViewRepository = new KVStoreRepository();
                ProxyMeta.getInstance().getTmManager().setRepository(newViewRepository);
                Map<String, Map<String, String>> viewCreateSqlMap = newViewRepository.getViewCreateSqlMap();
                ProxyMeta.getInstance().getTmManager().reloadViewMeta(viewCreateSqlMap);
                //init online status
                LOGGER.warn("Dble first reconnect to ucore ,online status rebuild");
            }
            OnlineStatus.getInstance().nodeListenerInitClusterOnline();
        }
    }
}
