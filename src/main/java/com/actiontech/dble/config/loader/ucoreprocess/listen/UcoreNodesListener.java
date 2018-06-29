package com.actiontech.dble.config.loader.ucoreprocess.listen;


import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.alarm.UcoreInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by szf on 2018/4/27.
 */
public class UcoreNodesListener implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreNodesListener.class);
    private long index = 0;

    @Override
    public void run() {
        for (; ; ) {
            try {
                UcoreInterface.SubscribeNodesInput subscribeNodesInput = UcoreInterface.SubscribeNodesInput.newBuilder().
                        setDuration(60).setIndex(index).build();
                UcoreInterface.SubscribeNodesOutput output = ClusterUcoreSender.subscribeNodes(subscribeNodesInput);
                if (index != output.getIndex()) {
                    index = output.getIndex();
                    List<String> ips = new ArrayList<>();
                    for (int i = 0; i < output.getIpsList().size(); i++) {
                        ips.add(output.getIps(i));
                    }
                    UcoreConfig.getInstance().setIpList(ips);
                    UcoreConfig.getInstance().setIp(String.join(",", ips));
                }
            } catch (Exception e) {
                LOGGER.warn("error in ucore nodes watch,try for another time");
            }
        }
    }
}
