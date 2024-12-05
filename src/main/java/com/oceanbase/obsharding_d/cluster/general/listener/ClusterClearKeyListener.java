/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.listener;

import com.oceanbase.obsharding_d.cluster.general.AbstractConsulSender;
import com.oceanbase.obsharding_d.util.exception.DetachedException;
import com.oceanbase.obsharding_d.cluster.general.bean.SubscribeRequest;
import com.oceanbase.obsharding_d.cluster.general.bean.SubscribeReturnBean;
import com.oceanbase.obsharding_d.cluster.general.response.ClusterXmlLoader;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.OriginClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/1/24.
 */
public class ClusterClearKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterClearKeyListener.class);

    private Map<String, ClusterXmlLoader> childService = new HashMap<>();


    private long index = 0;
    private AbstractConsulSender sender;
    private UcoreListenerHelper helper = new UcoreListenerHelper();

    public ClusterClearKeyListener(AbstractConsulSender sender) {
        this.sender = sender;
    }


    @Override
    public void run() {
        for (; ; ) {
            try {
                if (sender.isDetach()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
                    index = 0;
                    continue;
                }
                SubscribeRequest request = new SubscribeRequest();
                request.setIndex(index);
                request.setDuration(60);
                request.setPath(ClusterPathUtil.CONF_BASE_PATH);
                SubscribeReturnBean output = sender.subscribeKvPrefix(request);
                if (output.getIndex() != index) {
                    final Collection<OriginClusterEvent<?>> diffList = helper.getDiffList(output);
                    handle(diffList);
                    index = output.getIndex();
                }
            } catch (DetachedException e) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            } catch (IOException e) {
                if (!sender.isDetach()) {
                    LOGGER.info("error in deal with key,may be the ucore is shut down", e);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down", e);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }


    public void initForXml() {
        try {
            SubscribeRequest request = new SubscribeRequest();
            request.setIndex(0);
            request.setDuration(60);
            request.setPath(ClusterPathUtil.BASE_PATH);
            SubscribeReturnBean output = sender.subscribeKvPrefix(request);
            index = output.getIndex();
            final Collection<OriginClusterEvent<?>> diffList = helper.onFirst(output);
            handle(diffList);
        } catch (Exception e) {
            LOGGER.warn("error when start up OBsharding-D,ucore connect error");
        }
    }


    /**
     * handle the back data from the subscribe
     * if the config version changes,writeDirectly the file
     * or just start a new waiting
     */
    public void handle(Collection<OriginClusterEvent<?>> diffList) {
        try {
            diffList.stream().sorted(UcoreListenerHelper.sortRule()).forEach(event -> {
                ClusterXmlLoader x = childService.get(event.getPath());
                if (x != null) {
                    try {
                        x.notifyProcess(event, false);
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
