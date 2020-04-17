package com.actiontech.dble.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowCotrollerConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.WriteQueueFlowController;

import java.util.regex.Matcher;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlSet {

    private FlowControlSet() {

    }

    public static void execute(Matcher set, ManagerConnection mc) {

        String enable = set.group(2);
        String flowControlStart = set.group(4);
        String flowControlEnd = set.group(6);

        if (enable == null && flowControlStart == null && flowControlEnd == null) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the flow_control command");
            return;
        }
        FlowCotrollerConfig oldConfig = WriteQueueFlowController.getFlowCotrollerConfig();
        //create a new FlowCotrollerConfig
        FlowCotrollerConfig config =
                new FlowCotrollerConfig(enable == null ? oldConfig.isEnableFlowControl() : Boolean.valueOf(enable),
                        flowControlStart == null ? oldConfig.getStart() : Integer.parseInt(flowControlStart),
                        flowControlEnd == null ? oldConfig.getEnd() : Integer.parseInt(flowControlEnd));

        //check if the config is legal
        if (config.getEnd() < 0 || config.getStart() <= 0) {
            mc.writeErrMessage(ErrorCode.ER_YES, "The flowControlStartThreshold & flowControlStopThreshold must be positive integer");
            return;
        } else if (config.getEnd() >= config.getStart()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "The flowControlStartThreshold must bigger than flowControlStopThreshold");
            return;
        }

        WriteQueueFlowController.configChange(config);
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        packet.write(mc);
    }
}
