/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.DbleStartup;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static com.actiontech.dble.cluster.ClusterController.GENERAL_GRPC_TIMEOUT;
import java.net.*;
import java.util.*;

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
                    serverId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) + getLocalIPs();
                    alertComponentId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
                    Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getIpList().get(0),
                            Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).
                            usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel).withDeadlineAfter(GENERAL_GRPC_TIMEOUT, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                //config not ready yet
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
                    ClusterUcoreSender.alert(inpurt);
                } catch (Exception e1) {
                    LOGGER.info("connect to ucore error ", e1);
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

    private String getLocalIPs() {
        Set<String> ipList = new HashSet<>();
        Enumeration<?> network;
        List<NetworkInterface> netList = new ArrayList<>();
        try {
            network = NetworkInterface.getNetworkInterfaces();

            while (network.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) network.nextElement();

                if (ni.isLoopback()) {
                    continue;
                }
                netList.add(ni);
            }
            for (NetworkInterface list : netList) {
                Enumeration<?> card = list.getInetAddresses();
                while (card.hasMoreElements()) {
                    InetAddress ip = (InetAddress) card.nextElement();
                    if (!ip.isLoopbackAddress()) {
                        if (ip.getHostAddress().equalsIgnoreCase("127.0.0.1")) {
                            continue;
                        }
                    }
                    if (ip instanceof Inet6Address) {
                        continue;
                    }
                    if (ip instanceof Inet4Address) {
                        ipList.add(ip.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            System.out.println(e.getMessage());
        }
        StringBuilder sbIps = new StringBuilder("(");
        int i = 0;
        for (String ip : ipList) {
            if (i > 0) {
                sbIps.append(",");
            }
            sbIps.append(ip);
            i++;
        }
        sbIps.append(")");
        return sbIps.toString();
    }

}
