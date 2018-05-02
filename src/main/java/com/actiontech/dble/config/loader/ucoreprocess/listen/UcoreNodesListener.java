package com.actiontech.dble.config.loader.ucoreprocess.listen;


import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by szf on 2018/4/27.
 */
public class UcoreNodesListener implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreNodesListener.class);
    private UcoreGrpc.UcoreBlockingStub stub = null;
    private long index = 0;

    public void init() {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }


    @Override
    public void run() {
        for (; ; ) {
            try {
                UcoreInterface.SubscribeNodesInput subscribeNodesInput = UcoreInterface.SubscribeNodesInput.newBuilder().
                        setDuration(60).setIndex(index).build();
                UcoreInterface.SubscribeNodesOutput output = null;
                try {
                    output = stub.subscribeNodes(subscribeNodesInput);
                } catch (Exception e) {
                    //the first try failure ,try for all the other ucore ip
                    for (String ip : UcoreConfig.getInstance().getIpList()) {
                        ManagedChannel channel = null;
                        try {
                            channel = ManagedChannelBuilder.forAddress(ip,
                                    Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                            stub = UcoreGrpc.newBlockingStub(channel);
                            output = stub.subscribeNodes(subscribeNodesInput);
                            break;
                        } catch (Exception e2) {
                            LOGGER.info("try connection IP " + ip + " failure ", e2);
                            if (channel != null) {
                                channel.shutdownNow();
                            }
                        }
                    }
                    if (output == null) {
                        LOGGER.warn(AlarmCode.CORE_CLUSTER_WARN + " subscribeNodes error all ucore nodes connect failure");
                    }
                }
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
