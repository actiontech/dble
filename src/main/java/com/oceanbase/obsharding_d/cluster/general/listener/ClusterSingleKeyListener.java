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
import com.oceanbase.obsharding_d.cluster.values.OriginClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
                if (sender.isDetach()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
                    index = 0;
                    continue;
                }
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
