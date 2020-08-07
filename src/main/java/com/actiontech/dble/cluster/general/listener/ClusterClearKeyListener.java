/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/1/24.
 */
public class ClusterClearKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterClearKeyListener.class);

    private Map<String, ClusterXmlLoader> childService = new HashMap<>();

    private Map<String, String> cache = new HashMap<>();

    private long index = 0;
    private AbstractConsulSender sender;

    public ClusterClearKeyListener(AbstractConsulSender sender) {
        this.sender = sender;
    }


    @Override
    public void run() {
        for (; ; ) {
            try {
                SubscribeRequest request = new SubscribeRequest();
                request.setIndex(index);
                request.setDuration(60);
                request.setPath(ClusterPathUtil.CONF_BASE_PATH);
                SubscribeReturnBean output = sender.subscribeKvPrefix(request);
                if (output.getIndex() != index) {
                    Map<String, KvBean> diffMap = getDiffMapWithOrder(output);
                    handle(diffMap);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }


    private Map<String, KvBean> getDiffMapWithOrder(SubscribeReturnBean output) {
        Map<String, KvBean> diffMap = new LinkedHashMap<String, KvBean>();
        Map<String, String> newKeyMap = new LinkedHashMap<String, String>();

        KvBean reloadKv = null;

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            newKeyMap.put(output.getKeys(i), output.getValues(i));
            if (cache.get(output.getKeys(i)) != null) {
                if (!cache.get(output.getKeys(i)).equals(output.getValues(i))) {
                    if (output.getKeys(i).equalsIgnoreCase(ClusterPathUtil.getConfStatusPath())) {
                        reloadKv = new KvBean(output.getKeys(i), output.getValues(i), KvBean.UPDATE);
                    } else {
                        diffMap.put(output.getKeys(i), new KvBean(output.getKeys(i), output.getValues(i), KvBean.UPDATE));
                    }
                }
            } else {
                if (output.getKeys(i).equalsIgnoreCase(ClusterPathUtil.getConfStatusPath())) {
                    reloadKv = new KvBean(output.getKeys(i), output.getValues(i), KvBean.ADD);
                } else {
                    diffMap.put(output.getKeys(i), new KvBean(output.getKeys(i), output.getValues(i), KvBean.ADD));
                }
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                if (entry.getKey().equalsIgnoreCase(ClusterPathUtil.getConfStatusPath())) {
                    reloadKv = new KvBean(entry.getKey(), entry.getValue(), KvBean.DELETE);
                } else {
                    diffMap.put(entry.getKey(), new KvBean(entry.getKey(), entry.getValue(), KvBean.DELETE));
                }
            }
        }

        if (reloadKv != null) {
            diffMap.put(reloadKv.getKey(), reloadKv);
        }

        cache = newKeyMap;

        return diffMap;
    }

    public void initForXml() {
        try {
            SubscribeRequest request = new SubscribeRequest();
            request.setIndex(0);
            request.setDuration(60);
            request.setPath(ClusterPathUtil.BASE_PATH);
            SubscribeReturnBean output = sender.subscribeKvPrefix(request);
            index = output.getIndex();
            Map<String, KvBean> diffMap = new HashMap<String, KvBean>();
            for (int i = 0; i < output.getKeysCount(); i++) {
                diffMap.put(output.getKeys(i), new KvBean(output.getKeys(i), output.getValues(i), KvBean.ADD));
                cache.put(output.getKeys(i), output.getValues(i));
            }
            handle(diffMap);
        } catch (Exception e) {
            LOGGER.warn("error when start up dble,ucore connect error");
        }
    }


    /**
     * handle the back data from the subscribe
     * if the config version changes,writeDirectly the file
     * or just start a new waiting
     */
    public void handle(Map<String, KvBean> diffMap) {
        try {
            for (Map.Entry<String, KvBean> entry : diffMap.entrySet()) {
                ClusterXmlLoader x = childService.get(entry.getKey());
                if (x != null) {
                    x.notifyProcess(entry.getValue());
                }
            }
        } catch (Exception e) {
            LOGGER.warn(" ucore data parse to xml error ", e);
        }
    }


    /**
     * add ucoreXmlLoader into the watch list
     * every loader knows the path of it self
     *
     * @param loader
     * @param path
     */
    public void addChild(ClusterXmlLoader loader, String path) {
        this.childService.put(path, loader);
    }

    public ClusterXmlLoader getResponse(String key) {
        return this.childService.get(key);
    }

    public void initAllNode() throws Exception {
        for (Map.Entry<String, ClusterXmlLoader> service : childService.entrySet()) {
            try {
                service.getValue().notifyCluster();
            } catch (Exception e) {
                LOGGER.warn(" ClusterClearKeyListener init all node error:", e);
                throw e;
            }
        }
    }

    public long getIndex() {
        return index;
    }
}
