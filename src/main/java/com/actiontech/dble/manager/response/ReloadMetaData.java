/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public final class ReloadMetaData {
    private ReloadMetaData() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadMetaData.class);

    public static void execute(ManagerConnection c) {
        String msg = "data host has no write_host";
        boolean isOK = true;
        final ReentrantLock lock = DbleServer.getInstance().getTmManager().getMetaLock();
        lock.lock();
        try {
            String checkResult = DbleServer.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                LOGGER.warn(checkResult);
                c.writeErrMessage("HY000", checkResult, ErrorCode.ER_DOING_DDL);
                return;
            }
            try {
                if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
                    DbleServer.getInstance().reloadMetaData(DbleServer.getInstance().getConfig());
                    msg = "reload metadata success";
                }
            } catch (Exception e) {
                isOK = false;
                msg = "reload metadata failed," + e.toString();
            }
        } finally {
            lock.unlock();
        }
        if (isOK) {
            LOGGER.info(msg);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage(msg.getBytes());
            ok.write(c);
        } else {
            LOGGER.warn(msg);
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }
}
