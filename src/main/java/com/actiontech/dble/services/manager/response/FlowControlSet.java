package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.singleton.FlowController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowControlSet.class);
    private FlowControlSet() {

    }

    public static void execute(Matcher set, ManagerService service) {

        String enable = set.group(2);
        String flowControlHighLevel = set.group(4);
        String flowControlLowLevel = set.group(6);

        if (enable == null && flowControlHighLevel == null && flowControlLowLevel == null) {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the flow_control command");
            return;
        }
        FlowControllerConfig oldConfig = FlowController.getFlowControllerConfig();
        //create a new FlowControllerConfig
        FlowControllerConfig config =
                new FlowControllerConfig(enable == null ? oldConfig.isEnableFlowControl() : Boolean.parseBoolean(enable),
                        flowControlHighLevel == null ? oldConfig.getHighWaterLevel() : Integer.parseInt(flowControlHighLevel),
                        flowControlLowLevel == null ? oldConfig.getLowWaterLevel() : Integer.parseInt(flowControlLowLevel));

        //check if the config is legal
        if (config.getLowWaterLevel() < 0 || config.getHighWaterLevel() <= 0) {
            service.writeErrMessage(ErrorCode.ER_YES, "The flowControlHighLevel & flowControlLowLevel must be positive integer");
            return;
        } else if (config.getLowWaterLevel() >= config.getHighWaterLevel()) {
            service.writeErrMessage(ErrorCode.ER_YES, "The flowControlHighLevel must bigger than flowControlLowLevel");
            return;
        }
        try {
            List<Pair<String, String>> props = new ArrayList<>();
            props.add(new Pair<>("enableFlowControl", String.valueOf(config.isEnableFlowControl())));
            props.add(new Pair<>("flowControlHighLevel", Integer.toString(config.getHighWaterLevel())));
            props.add(new Pair<>("flowControlLowLevel", Integer.toString(config.getLowWaterLevel())));
            WriteDynamicBootstrap.getInstance().changeValue(props);
        } catch (IOException e) {
            String msg = "flow_control @@set failed";
            LOGGER.warn(service + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        FlowController.configChange(config);
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }
}
