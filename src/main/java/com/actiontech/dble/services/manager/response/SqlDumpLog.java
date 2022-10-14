package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.sqldump.SqlDumpLogHelper;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.util.StringUtil;
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
