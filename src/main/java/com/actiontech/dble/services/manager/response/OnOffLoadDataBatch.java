/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.status.LoadDataBatch;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
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
