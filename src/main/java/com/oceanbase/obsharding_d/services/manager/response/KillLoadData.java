/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.server.status.LoadDataBatch;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author mycat
 */
public final class KillLoadData {
    private KillLoadData() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KillLoadData.class);

    public static void response(ManagerService service) {
        LoadDataBatch.getInstance().cleanAll();
        LOGGER.info(service + " kill @@load_data success by manager");
        try {
            deleteFile(SystemConfig.getInstance().getHomePath() + File.separator + "temp");
        } catch (Exception e) {
            String msg = " kill @@load_data failed";
            LOGGER.warn(service + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("kill @@load_data success".getBytes());
        ok.write(service.getConnection());
    }

    private static void deleteFile(String dirPath) {
        File fileDirToDel = new File(dirPath);
        if (!fileDirToDel.exists()) {
            return;
        }
        if (fileDirToDel.isFile()) {
            fileDirToDel.delete();
            return;
        }
        File[] fileList = fileDirToDel.listFiles();
        if (fileList != null) {
            for (File file : fileList) {
                if (file.isFile() && file.exists()) {
                    file.delete();
                } else if (file.isDirectory()) {
                    deleteFile(file.getAbsolutePath());
                    file.delete();
                }
            }
        }
        fileDirToDel.delete();
    }


}
