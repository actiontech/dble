package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlHandler {
    private static final Pattern FLOW_CONTROL_LIST = Pattern.compile("^\\s*flow_control\\s*@@list\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FLOW_CONTROL_SET = Pattern.compile("^\\s*flow_control\\s+@@set(\\s+enableFlowControl\\s*=\\s*(true|false))?" +
            "(\\s+flowControlStart\\s*=\\s*(\\d*))?(\\s+flowControlEnd\\s*=\\s*(\\d*))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FLOW_CONTROL_SHOW = Pattern.compile("^\\s*flow_control\\s*@@show\\s*$", Pattern.CASE_INSENSITIVE);

    private FlowControlHandler() {

    }

    public static void handle(String stmt, ManagerConnection c) {
        Matcher list = FLOW_CONTROL_LIST.matcher(stmt);
        Matcher set = FLOW_CONTROL_SET.matcher(stmt);
        Matcher show = FLOW_CONTROL_SHOW.matcher(stmt);
        if (list.matches()) {
            FlowControlList.execute(c);
        } else if (set.matches()) {
            FlowControlSet.execute(set, c);
        } else if (show.matches()) {
            FlowControlShow.execute(c);
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the flow_control command");
        }
    }
}
