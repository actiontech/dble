/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import com.actiontech.dble.cluster.values.OriginClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
    private UcoreListenerHelper helper = new UcoreListenerHelper();


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
                    final Collection<OriginClusterEvent<?>> diffList = helper.getDiffList(output);
                    handle(diffList);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }

    public void handle(Collection<OriginClusterEvent<?>> diffList) {
        try {
            diffList.stream().sorted(UcoreListenerHelper.sortRule()).forEach(event -> {
                try {
                    child.notifyProcess(event, true);

                } catch (Exception e) {
                    LOGGER.warn(" ucore event handle error", e);
                }

            });

        } catch (Exception e) {
            LOGGER.warn(" ucore event handle error", e);
        }

    }


}
