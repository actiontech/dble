/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.ha.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szf on 2019/10/22.
 */
public final class DbGroupHAHandler {

    private static final String DB_NAME_FORMAT = "a-zA-Z_0-9\\-\\."; //nmtoken
    private static final Pattern PATTERN_DH_DISABLE = Pattern.compile("@@disable\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(instance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_ENABLE = Pattern.compile("@@enable\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(instance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_SWITCH = Pattern.compile("@@switch\\s*name\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*master\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_DH_EVENTS = Pattern.compile("@@events\\s*$", Pattern.CASE_INSENSITIVE);

    private DbGroupHAHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        String options = stmt.substring(offset).trim();
        service.getClusterDelayService().markDoingOrDelay(true);
        Matcher disable = PATTERN_DH_DISABLE.matcher(options);
        Matcher enable = PATTERN_DH_ENABLE.matcher(options);
        Matcher switcher = PATTERN_DH_SWITCH.matcher(options);
        Matcher event = PATTERN_DH_EVENTS.matcher(options);
        PacketResult packetResult = new PacketResult();
        if (disable.matches()) {
            DbGroupHaDisable.execute(disable, packetResult);
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg());
        } else if (enable.matches()) {
            DbGroupHaEnable.execute(enable, packetResult);
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg());
        } else if (switcher.matches()) {
            DbGroupHaSwitch.execute(switcher, packetResult);
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg());
        } else if (event.matches()) {
            DbGroupHaEvents.execute(service);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the dbGroup command");
        }
    }

    private static void writePacket(boolean isSuccess, ManagerService service, String errorMsg) {
        if (isSuccess) {
            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(service.getConnection());
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, errorMsg);
        }
    }
}
