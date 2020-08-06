package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.singleton.WriteQueueFlowController;
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
        String flowControlStart = set.group(4);
        String flowControlEnd = set.group(6);

        if (enable == null && flowControlStart == null && flowControlEnd == null) {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the flow_control command");
            return;
        }
        FlowControllerConfig oldConfig = WriteQueueFlowController.getFlowCotrollerConfig();
        //create a new FlowCotrollerConfig
        FlowControllerConfig config =
                new FlowControllerConfig(enable == null ? oldConfig.isEnableFlowControl() : Boolean.valueOf(enable),
                        flowControlStart == null ? oldConfig.getStart() : Integer.parseInt(flowControlStart),
                        flowControlEnd == null ? oldConfig.getEnd() : Integer.parseInt(flowControlEnd));

        //check if the config is legal
        if (config.getEnd() < 0 || config.getStart() <= 0) {
            service.writeErrMessage(ErrorCode.ER_YES, "The flowControlStartThreshold & flowControlStopThreshold must be positive integer");
            return;
        } else if (config.getEnd() >= config.getStart()) {
            service.writeErrMessage(ErrorCode.ER_YES, "The flowControlStartThreshold must bigger than flowControlStopThreshold");
            return;
        }
        try {
            List<Pair<String, String>> props = new ArrayList<>();
            props.add(new Pair<>("enableFlowControl", String.valueOf(config.isEnableFlowControl())));
            props.add(new Pair<>("flowControlStartThreshold", Integer.toString(config.getStart())));
            props.add(new Pair<>("flowControlStopThreshold", Integer.toString(config.getEnd())));
            WriteDynamicBootstrap.getInstance().changeValue(props);
        } catch (IOException e) {
            String msg = "flow_control @@set failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        WriteQueueFlowController.configChange(config);
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }
}
