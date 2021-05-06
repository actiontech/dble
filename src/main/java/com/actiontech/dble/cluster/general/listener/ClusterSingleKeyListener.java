/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.ChangeType;
import com.actiontech.dble.cluster.ClusterEvent;
import com.actiontech.dble.cluster.ClusterValue;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import com.actiontech.dble.cluster.values.AnyType;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

    private Map<String, ClusterEvent<?>> cache = new HashMap<>();


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

    public void handle(Map<String, ClusterEvent<?>> diffMap) {
        try {
            diffMap.entrySet().stream().sorted((e1, e2) -> Longs.compare(e1.getValue().getValue().getCreatedAt(), e2.getValue().getValue().getCreatedAt())).forEach(entry -> {
                try {
                    child.notifyProcess(entry.getValue(), true);

                } catch (Exception e) {
                    LOGGER.warn(" ucore event handle error", e);
                }

            });

        } catch (Exception e) {
            LOGGER.warn(" ucore event handle error", e);
        }

    }


    private Map<String, ClusterEvent<?>> getDiffMap(SubscribeReturnBean output) {
        Map<String, ClusterEvent<?>> diffMap = new HashMap<>();
        Map<String, ClusterEvent<?>> newKeyMap = new HashMap<>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            final ClusterValue<AnyType> clusterValue = ClusterValue.readFromJson(output.getValues(i), AnyType.class);
            newKeyMap.put(output.getKeys(i), new ClusterEvent<>(output.getKeys(i), clusterValue, ChangeType.UPDATED));
            if (cache.get(output.getKeys(i)) != null) {
                final ClusterValue<?> value = cache.get(output.getKeys(i)).getValue();
                if ((!Objects.equals(value.getInstanceName(), clusterValue.getInstanceName())) || (!Objects.equals(value.getCreatedAt(), clusterValue.getCreatedAt()))) {
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
}
