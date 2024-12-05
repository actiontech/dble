/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;


import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.statistic.stat.QueryConditionAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReloadQueryCf {
    private ReloadQueryCf() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadQueryCf.class);

    public static void execute(ManagerService service, String cf) {

        if (cf == null) {
            cf = "NULL";
        }

        QueryConditionAnalyzer.getInstance().setCf(cf);

        LOGGER.info(String.valueOf(service) + "Reset show  @@sql.condition=" + cf + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Reset show  @@sql.condition success".getBytes());
        ok.write(service.getConnection());
    }

}
