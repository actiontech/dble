/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.DbleStartup;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

/**
 * Created by szf on 2017/12/4.
 */
@Plugin(name = "AlarmAppender", category = "Core", elementType = "appender", printObject = true)
public class AlarmAppender extends AbstractAppender {

    private static int grpcLevel = 300;
    private static String serverId = "";
    private static String alertComponentId = "";


    private static final String USHARD_CODE = "ushard";
    private static UcoreGrpc.UcoreBlockingStub stub = null;

    /**
     * method to init the whole appender
     *
     * @param name
     * @param layout
     */
    protected AlarmAppender(String name,
                            Layout<? extends Serializable> layout) {
        super(name, null, layout, true);
    }

    @Override
    public void append(LogEvent event) {
        if (stub == null && DbleStartup.isInitZKend()) {
            //only if the dbleserver init config file finished than the config can be use for alert
            try {
                if (DbleServer.getInstance().isUseUcore()) {
                    grpcLevel = 300;
                    serverId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
                    alertComponentId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_CLUSTERID);
                    Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).
                            usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                }
            } catch (Exception e) {
                //config not ready yeat
                return;
            }
        }
        if (stub != null) {
            send(event);
        }

    }

    public void send(LogEvent event) {
        if (grpcLevel >= event.getLevel().intLevel()) {
            String data = new String(getLayout().toByteArray(event));
            String[] d = data.split("::");
            if (d.length >= 2) {
                String level = event.getLevel().intLevel() == 300 ? "WARN" : "CRITICAL";
                UcoreInterface.AlertInput inpurt = UcoreInterface.AlertInput.newBuilder().
                        setCode(d[0]).
                        setDesc(d[1]).
                        setLevel(level).
                        setSourceComponentType(USHARD_CODE).
                        setSourceComponentId(alertComponentId).
                        setAlertComponentId(alertComponentId).
                        setAlertComponentType(USHARD_CODE).
                        setServerId(serverId).
                        setTimestampUnix(System.currentTimeMillis() * 1000000).
                        build();

                try {
                    stub.alert(inpurt);
                } catch (Exception e1) {
                    for (String ip : UcoreConfig.getInstance().getIpList()) {
                        ManagedChannel channel = null;
                        try {
                            channel = ManagedChannelBuilder.forAddress(ip,
                                    Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
                            stub = UcoreGrpc.newBlockingStub(channel);
                            stub.alert(inpurt);
                            return;
                        } catch (Exception e2) {
                            LOGGER.info("connect to ucore error ", e2);
                            if (channel != null) {
                                channel.shutdownNow();
                            }
                        }
                    }
                }
            }
        }
    }


    @PluginFactory
    public static AlarmAppender createAppender(
            @PluginAttribute("name") String name
    ) {
        if (name == null) {
            return null;
        }
        Layout<? extends Serializable> layout = PatternLayout.createDefaultLayout();
        return new AlarmAppender(name, layout);
    }


}
