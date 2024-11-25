/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.log.sqldump.SqlDumpLogHelper;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlDumpLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpLog.class);

    public static class OnOff {

        public static void execute(ManagerService service, boolean isOn) {
            String onOffStatus = isOn ? "enable" : "disable";
            try {
                WriteDynamicBootstrap.getInstance().changeValue("enableSqlDumpLog", isOn ? "1" : "0");
            } catch (Exception ex) {
                LOGGER.warn("enable/disable SqlDumpLog failed, exception：", ex);
                service.writeErrMessage(ErrorCode.ER_YES, onOffStatus + " SqlDumpLog failed");
                return;
            }
            String errMsg = SqlDumpLogHelper.onOff(isOn);
            if (StringUtil.isEmpty(errMsg)) {
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.write(service.getConnection());
            } else {
                service.writeErrMessage(ErrorCode.ER_YES, errMsg);
            }
        }
    }
}
