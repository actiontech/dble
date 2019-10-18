package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.DataHostDisable;
import com.actiontech.dble.manager.response.DataHostEnable;
import com.actiontech.dble.manager.response.DataHostStatus;
import com.actiontech.dble.manager.response.DataHostSwitch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2019/10/22.
 */
public final class DataHostHandler {


    private static final Pattern PATTERN_DH_DISABLE = Pattern.compile("^\\s*dataHost\\s*@@disable\\s*name\\s*=\\s*'([a-zA-Z_0-9\\-]+)'" +
            "\\s*(node\\s*=\\s*'([a-zA-Z_0-9\\-\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_ENABLE = Pattern.compile("^\\s*dataHost\\s*@@enable\\s*name\\s*=\\s*'([a-zA-Z_0-9\\-]+)'" +
            "\\s*(node\\s*=\\s*'([a-zA-Z_0-9\\-\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_SWITCH = Pattern.compile("^dataHost\\s*@@switch\\s*name\\s*=\\s*'([a-zA-Z_0-9\\-\\,]+)'" +
            "\\s*master\\s*=\\s*'([a-zA-Z_0-9\\-\\,]+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_STATUS = Pattern.compile("^dataHost\\s*@@(status_all\\s*|(status\\s*name\\s*=\\s*'([a-zA-Z_0-9\\-\\,]+)'\\s*))$", Pattern.CASE_INSENSITIVE);

    private DataHostHandler() {
    }

    public static void handle(String stmt, ManagerConnection c) {
        Matcher disable = PATTERN_DH_DISABLE.matcher(stmt);
        Matcher enable = PATTERN_DH_ENABLE.matcher(stmt);
        Matcher switcher = PATTERN_DH_SWITCH.matcher(stmt);
        Matcher status = PATTERN_DH_STATUS.matcher(stmt);
        if (disable.matches()) {
            DataHostDisable.execute(disable, c);
        } else if (enable.matches()) {
            DataHostEnable.execute(enable, c);
        } else if (switcher.matches()) {
            DataHostSwitch.execute(switcher, c);
        } else if (status.matches()) {
            DataHostStatus.execute(status, c);
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the dataHost command");
        }

    }


}
