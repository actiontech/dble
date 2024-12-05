/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.FlowControlList;
import com.oceanbase.obsharding_d.services.manager.response.FlowControlSet;
import com.oceanbase.obsharding_d.services.manager.response.FlowControlShow;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2020/4/10.
 */
public final class FlowControlHandler {
    private static final Pattern FLOW_CONTROL_LIST = Pattern.compile("^\\s*@@list\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FLOW_CONTROL_SET = Pattern.compile("^\\s*@@set(\\s+enableFlowControl\\s*=\\s*(true|false))?" +
            "(\\s+flowControlHighLevel\\s*=\\s*(\\d*))?(\\s+flowControlLowLevel\\s*=\\s*(\\d*))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FLOW_CONTROL_SHOW = Pattern.compile("^\\s*@@show\\s*$", Pattern.CASE_INSENSITIVE);

    private FlowControlHandler() {

    }

    public static void handle(String stmt, ManagerService service, int offset) {
        String flowOptions = stmt.substring(offset).trim();
        Matcher list = FLOW_CONTROL_LIST.matcher(flowOptions);
        Matcher set = FLOW_CONTROL_SET.matcher(flowOptions);
        Matcher show = FLOW_CONTROL_SHOW.matcher(flowOptions);
        if (list.matches()) {
            FlowControlList.execute(service);
        } else if (set.matches()) {
            FlowControlSet.execute(set, service);
        } else if (show.matches()) {
            FlowControlShow.execute(service);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the flow_control command");
        }
    }
}
