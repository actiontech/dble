/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.AnyType;
import com.actiontech.dble.cluster.values.ChangeType;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/1/24.
 */
public class ClusterClearKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterClearKeyListener.class);

    private Map<String, ClusterXmlLoader> childService = new HashMap<>();

    private Map<String, ClusterEvent<?>> cache = new HashMap<>();

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
                    Map<String, ClusterEvent<?>> diffMap = getDiffMap(output);
                    handle(diffMap);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }

    private Map<String, ClusterEvent<?>> getDiffMap(SubscribeReturnBean output) {
        Map<String, ClusterEvent<?>> diffMap = new HashMap<>();
        Map<String, ClusterEvent<?>> newKeyMap = new HashMap<>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            final ClusterValue<AnyType> clusterValue = ClusterValue.readFromJson(output.getValues(i), AnyType.class);
            //noinspection deprecation
            newKeyMap.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.UPDATED));
            if (cache.get(output.getKeys(i)) != null) {
                final ClusterValue<?> value = cache.get(output.getKeys(i)).getValue();
                if ((!Objects.equals(value.getInstanceName(), clusterValue.getInstanceName())) || (!Objects.equals(value.getCreatedAt(), clusterValue.getCreatedAt()))) {
                    //noinspection deprecation
                    diffMap.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.UPDATED));
                }
            } else {
                diffMap.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.ADDED));
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, ClusterEvent<?>> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                diffMap.put(entry.getKey(), new ClusterEvent<>(entry.getKey(), entry.getValue().getValue(), ChangeType.REMOVED));
            }
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
            Map<String, ClusterEvent<?>> diffMap = new HashMap<>();
            for (int i = 0; i < output.getKeysCount(); i++) {
                final ClusterValue<AnyType> clusterValue = ClusterValue.readFromJson(output.getValues(i), AnyType.class);
                diffMap.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.ADDED));
                cache.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.ADDED));
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
    public void handle(Map<String, ClusterEvent<?>> diffMap) {
        try {
            diffMap.entrySet().stream().sorted((e1, e2) -> Longs.compare(e1.getValue().getValue().getCreatedAt(), e2.getValue().getValue().getCreatedAt())).forEach(entry -> {
                ClusterXmlLoader x = childService.get(entry.getKey());
                if (x != null) {
                    try {
                        x.notifyProcess(entry.getValue(), false);
                    } catch (Exception e) {
                        LOGGER.warn(" ucore data parse to xml error", e);
                    }
                }
            });
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
