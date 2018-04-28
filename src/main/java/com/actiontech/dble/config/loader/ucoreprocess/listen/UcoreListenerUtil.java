package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by szf on 2018/4/27.
 */
public class UcoreListenerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreListenerUtil.class);
    private UcoreGrpc.UcoreBlockingStub stub = null;

    public UcoreListenerUtil() {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }


    public UcoreInterface.SubscribeKvPrefixOutput subscribeKvPrefix(UcoreInterface.SubscribeKvPrefixInput input) throws IOException {
        try {
            UcoreInterface.SubscribeKvPrefixOutput output = stub.subscribeKvPrefix(input);
            return output;
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                try {
                    Channel channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                    UcoreInterface.SubscribeKvPrefixOutput output = stub.subscribeKvPrefix(input);
                    return output;
                } catch (Exception e2) {
                    LOGGER.info("connect to ucore at " + ip + " failure");
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
    }

}
