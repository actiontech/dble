/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.server.status.LoadDataBatch;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class OnOffLoadDataBatch {
    private OnOffLoadDataBatch() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffLoadDataBatch.class);

    public static void execute(ManagerService service, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("enableBatchLoadData", isOn ? "1" : "0");
        } catch (IOException e) {
            String msg = onOffStatus + " load_data_batch failed";
            LOGGER.warn(service + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        LoadDataBatch.getInstance().setEnableBatchLoadData(isOn);
        LOGGER.info(service + " " + onOffStatus + " load_data_batch success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " load_data_batch success").getBytes());
        ok.write(service.getConnection());
    }

}
