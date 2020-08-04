package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.ha.DbGroupHaDisable;
import com.actiontech.dble.services.manager.response.ha.DbGroupHaEnable;
import com.actiontech.dble.services.manager.response.ha.DbGroupHaEvents;
import com.actiontech.dble.services.manager.response.ha.DbGroupHaSwitch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHAHandler {

    public static final String DB_NAME_FORMAT = "a-zA-Z_0-9\\-\\."; //nmtoken
    private static final Pattern PATTERN_DH_DISABLE = Pattern.compile("^\\s*dbGroup\\s*@@disable\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(instance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_ENABLE = Pattern.compile("^\\s*dbGroup\\s*@@enable\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(instance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_SWITCH = Pattern.compile("^\\s*dbGroup\\s*@@switch\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*master\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_EVENTS = Pattern.compile("^dbGroup\\s*@@events\\s*$", Pattern.CASE_INSENSITIVE);

    private DbGroupHAHandler() {
    }

    public static void handle(String stmt, ManagerService service) {
        Matcher disable = PATTERN_DH_DISABLE.matcher(stmt);
        Matcher enable = PATTERN_DH_ENABLE.matcher(stmt);
        Matcher switcher = PATTERN_DH_SWITCH.matcher(stmt);
        Matcher event = PATTERN_DH_EVENTS.matcher(stmt);
        if (disable.matches()) {
            DbGroupHaDisable.execute(disable, service);
        } else if (enable.matches()) {
            DbGroupHaEnable.execute(enable, service);
        } else if (switcher.matches()) {
            DbGroupHaSwitch.execute(switcher, service);
        } else if (event.matches()) {
            DbGroupHaEvents.execute(service);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the dbGroup command");
        }

    }


}
