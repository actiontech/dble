package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.actiontech.dble.cluster.ClusterController.GRPC_SUBTIMEOUT;

/**
 * Created by szf on 2018/4/27.
 */
public class UcoreListenerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreListenerUtil.class);
    private UcoreGrpc.UcoreBlockingStub stub = null;

    public UcoreListenerUtil() {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS);
    }


    public UcoreInterface.SubscribeKvPrefixOutput subscribeKvPrefix(UcoreInterface.SubscribeKvPrefixInput input) throws IOException {
        try {
            UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
            return output;
        } catch (Exception e1) {
            for (String ip : UcoreConfig.getInstance().getIpList()) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(ip,
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS);
                    UcoreInterface.SubscribeKvPrefixOutput output = stub.withDeadlineAfter(GRPC_SUBTIMEOUT, TimeUnit.SECONDS).subscribeKvPrefix(input);
                    return output;

                } catch (Exception e2) {
                    LOGGER.info("connect to ucore at " + ip + " failure", e2);
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }
        }
        throw new IOException("ALL the ucore connect failure");
    }

}
