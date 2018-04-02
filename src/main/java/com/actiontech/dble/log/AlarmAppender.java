/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.DbleStartup;
import com.actiontech.dble.config.model.AlarmConfig;
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

/**
 * Created by szf on 2017/12/4.
 */
@Plugin(name = "AlarmAppender", category = "Core", elementType = "appender", printObject = true)
public class AlarmAppender extends AbstractAppender {

    private static String grpcUrl = "";
    private static int port = 0;
    private static int grpcLevel = 0;
    private static String serverId = "";
    private static String alertComponentId = "";

    private static String grpcUrlOld = "";
    private static int portOld = 0;
    private static int grpcLevelOld = 0;
    private static String serverIdOld = "";
    private static String alertComponentIdOld = "";


    private static String ushardCode = "";
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
                AlarmConfig config = DbleServer.getInstance().getConfig().getAlarm();
                if (config != null && config.getUrl() != null) {
                    grpcLevel = "error".equalsIgnoreCase(config.getLevel()) ? 200 : 300;
                    serverId = config.getServerId();
                    port = Integer.parseInt(config.getPort());
                    grpcUrl = config.getUrl();
                    alertComponentId = config.getComponentId();
                    ushardCode = config.getComponentType();
                    Channel channel = ManagedChannelBuilder.forAddress(grpcUrl, port).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                }
            } catch (Exception e) {
                //config not ready yeat
                return;
            }
        }
        if (stub != null) {
            try {
                send(event);
            }catch (Exception e){
                //error when send info to ucore , try again
                Channel channel = ManagedChannelBuilder.forAddress(grpcUrl, port).usePlaintext(true).build();
                stub = UcoreGrpc.newBlockingStub(channel);
                send(event);
            }
        }

    }

    public void send(LogEvent event){
        if (grpcLevel >= event.getLevel().intLevel()) {
            String data = new String(getLayout().toByteArray(event));
            String[] d = data.split("::");
            if (d.length >= 2) {
                String level = event.getLevel().intLevel() == 300 ? "WARN" : "CRITICAL";
                UcoreInterface.AlertInput inpurt = UcoreInterface.AlertInput.newBuilder().
                        setCode(d[0]).
                        setDesc(d[1]).
                        setLevel(level).
                        setSourceComponentType(ushardCode).
                        setSourceComponentId(alertComponentId).
                        setAlertComponentId(alertComponentId).
                        setAlertComponentType(ushardCode).
                        setServerId(serverId).
                        setTimestampUnix(System.currentTimeMillis() * 1000000).
                        build();
                stub.alert(inpurt);
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


    /**
     * refresh config of alarm address and re create the stub
     */
    public static void refreshConfig() {
        try {
            AlarmConfig config = DbleServer.getInstance().getConfig().getAlarm();
            if (config != null) {
                //put the old config into  _old
                grpcUrlOld = grpcUrl;
                serverIdOld = serverId;
                alertComponentIdOld = alertComponentId;
                portOld = port;
                grpcUrlOld = grpcUrl;
                grpcLevelOld = grpcLevel;

                grpcLevel = "error".equalsIgnoreCase(config.getLevel()) ? 200 : 300;
                serverId = config.getServerId();
                port = Integer.parseInt(config.getPort());
                grpcUrl = config.getUrl();
                alertComponentId = config.getComponentId();
                if (port != portOld || !grpcUrlOld.equals(grpcUrl)) {
                    Channel channel = ManagedChannelBuilder.forAddress(grpcUrl, port).usePlaintext(true).build();
                    stub = UcoreGrpc.newBlockingStub(channel);
                }
            } else {
                stub = null;
            }
        } catch (Exception e) {
            //config not ready yeat
            return;
        }
    }


    /**
     * if the config is rollback the config of dbleAppender should be rollback too
     */
    public static void rollbackConfig() {
        if (stub == null && (grpcUrlOld == null && "".equals(grpcUrlOld))) {
            grpcUrl = grpcUrlOld;
            serverId = serverIdOld;
            alertComponentId = alertComponentIdOld;
            port = portOld;
            grpcUrl = grpcUrlOld;
            grpcLevel = grpcLevelOld;
            return;
        } else {
            grpcUrl = grpcUrlOld;
            serverId = serverIdOld;
            alertComponentId = alertComponentIdOld;
            port = portOld;
            grpcUrl = grpcUrlOld;
            try {
                Channel channel = ManagedChannelBuilder.forAddress(grpcUrl, port).usePlaintext(true).build();
                stub = UcoreGrpc.newBlockingStub(channel);
            } catch (Exception e) {
                return;
            }
        }
    }
}
