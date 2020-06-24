/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general;

import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.singleton.ProxyMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2019/3/11.
 */
public abstract class AbstractClusterSender implements ClusterSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusterSender.class);


    public String waitingForAllTheNode(String checkString, String path) {
        Map<String, String> expectedMap = ClusterToXml.getOnlineMap();
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

    public boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        Map<String, String> currentMap = ClusterToXml.getOnlineMap();
        checkOnline(expectedMap, currentMap);
        List<KvBean> responseList = ClusterHelper.getKVPath(path);
        boolean flag = false;
        if (expectedMap.size() == 0) {
            if (errorMsg != null) {
                errorMsg.append("All online key droped, other instance config may out of sync, try again");
            }
            return true;
        }
        for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
            flag = false;
            for (KvBean kvBean : responseList) {
                String responseNode = last(kvBean.getKey().split("/"));
                if (last(entry.getKey().split("/")).
                        equals(responseNode)) {
                    if (checkString != null) {
                        if (!checkString.equals(kvBean.getValue())) {
                            if (errorMsg != null) {
                                errorMsg.append(responseNode).append(":").append(kvBean.getValue()).append(";");
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


    public void checkOnline(Map<String, String> expectedMap, Map<String, String> currentMap) {
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
                LOGGER.warn("Dble first reconnect to ucore ,local view repository change to CKVStoreRepository");
                Repository newViewRepository = new CKVStoreRepository();
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
