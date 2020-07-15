/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * Created by szf on 2018/2/2.
 */
public class ClusterSingleKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSingleKeyListener.class);
    private long index = 0;
    ClusterXmlLoader child;
    String path;
    private AbstractConsulSender sender;

    private Map<String, String> cache = new HashMap<>();


    public ClusterSingleKeyListener(String path, ClusterXmlLoader child, AbstractConsulSender sender) {
        this.child = child;
        this.path = path;
        this.sender = sender;
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                SubscribeRequest request = new SubscribeRequest();
                request.setIndex(index);
                request.setDuration(60);
                request.setPath(path);
                SubscribeReturnBean output = sender.subscribeKvPrefix(request);
                if (output.getIndex() != index) {
                    Map<String, KvBean> diffMap = getDiffMap(output);
                    handle(diffMap);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }

    public void handle(Map<String, KvBean> diffMap) {
        try {
            for (Map.Entry<String, KvBean> entry : diffMap.entrySet()) {
                child.notifyProcess(entry.getValue());
            }
        } catch (Exception e) {
            LOGGER.warn(" ucore event handle error", e);
        }
    }


    private Map<String, KvBean> getDiffMap(SubscribeReturnBean output) {
        Map<String, KvBean> diffMap = new HashMap<String, KvBean>();
        Map<String, String> newKeyMap = new HashMap<String, String>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            newKeyMap.put(output.getKeys(i), output.getValues(i));
            if (cache.get(output.getKeys(i)) != null) {
                if (!cache.get(output.getKeys(i)).equals(output.getValues(i))) {
                    diffMap.put(output.getKeys(i), new KvBean(output.getKeys(i), output.getValues(i), KvBean.UPDATE));
                }
            } else {
                diffMap.put(output.getKeys(i), new KvBean(output.getKeys(i), output.getValues(i), KvBean.ADD));
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                diffMap.put(entry.getKey(), new KvBean(entry.getKey(), entry.getValue(), KvBean.DELETE));
            }
        }

        cache = newKeyMap;

        return diffMap;
    }
}
