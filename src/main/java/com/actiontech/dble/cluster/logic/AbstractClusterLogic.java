/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ClusterEntry;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.cluster.values.OnlineType;
import com.actiontech.dble.singleton.TraceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class AbstractClusterLogic extends GeneralClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(AbstractClusterLogic.class);
    protected final ClusterOperation type;
    protected final ClusterHelper clusterHelper;

    public AbstractClusterLogic(ClusterOperation type) {
        this.type = type;
        this.clusterHelper = ClusterHelper.getInstance(type);
    }


    public <T> List<ClusterEntry<T>> getKVBeanOfChildPath(ChildPathMeta<T> meta) throws Exception {
        List<ClusterEntry<T>> allList = clusterHelper.getKVPath(meta);
        int parentHeight = getPathHeight(meta.getPath());
        Iterator<ClusterEntry<T>> iter = allList.iterator();
        while (iter.hasNext()) {
            ClusterEntry<T> bean = iter.next();
            String[] key = bean.getKey().split(ClusterPathUtil.SEPARATOR);
            if (key.length != parentHeight + 1) {
                iter.remove();
            }
        }
        return allList;
    }

    public boolean checkResponseForOneTime(String path, Map<String, OnlineType> expectedMap, @Nonnull StringBuilder errorMsg) {
        if (expectedMap.size() == 0) {
            return true;
        }
        Map<String, OnlineType> currentMap = ClusterHelper.getOnlineMap();
        checkOnline(expectedMap, currentMap);
        List<ClusterEntry<FeedBackType>> responseList;
        try {
            responseList = getKVBeanOfChildPath(ChildPathMeta.of(path, FeedBackType.class));
        } catch (Exception e) {
            LOGGER.warn("checkResponseForOneTime error :", e);
            errorMsg.append(e.getMessage());
            return true;
        }
        if (expectedMap.size() == 0) {
            errorMsg.append("All online key dropped, other instance config may out of sync, try again manually");
            return true;
        }

        boolean flag = true;
        for (Map.Entry<String, OnlineType> entry : expectedMap.entrySet()) {
            boolean found = false;
            for (ClusterEntry<FeedBackType> kvBean : responseList) {
                String responseNode = lastItemOfArray(kvBean.getKey().split(ClusterPathUtil.SEPARATOR));
                if (lastItemOfArray(entry.getKey().split(ClusterPathUtil.SEPARATOR)).equals(responseNode)) {
                    if (!kvBean.getValue().getData().isSuccess()) {
                        errorMsg.append(responseNode).append(":").append(kvBean.getValue().getData().getMessage()).append(";");
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                flag = false;
            }
        }

        return flag;
    }

    private static <T> T lastItemOfArray(T[] array) {
        return array[array.length - 1];
    }


    public String waitingForAllTheNode(String path) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("wait-for-others-cluster");
        try {
            Map<String, OnlineType> expectedMap = ClusterHelper.getOnlineMap();
            StringBuilder errorMsg = new StringBuilder();
            for (; ; ) {
                errorMsg.setLength(0);
                if (checkResponseForOneTime(path, expectedMap, errorMsg)) {
                    break;
                }
            }
            return errorMsg.length() <= 0 ? null : errorMsg.toString();
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }


    private static void checkOnline(Map<String, OnlineType> expectedMap, Map<String, OnlineType> currentMap) {
        expectedMap.entrySet().removeIf(entry -> !currentMap.containsKey(entry.getKey()) ||
                (currentMap.containsKey(entry.getKey()) && !currentMap.get(entry.getKey()).equals(entry.getValue())));

        for (Map.Entry<String, OnlineType> entry : currentMap.entrySet()) {
            if (!expectedMap.containsKey(entry.getKey())) {
                LOGGER.warn("NODE " + entry.getKey() + " IS NOT EXPECTED TO BE ONLINE,PLEASE CHECK IT ");
            }
        }
    }
}
