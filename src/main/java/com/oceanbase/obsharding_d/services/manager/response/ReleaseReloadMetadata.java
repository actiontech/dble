/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ReloadManager;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;

/**
 * Created by szf on 2019/7/16.
 */
public final class ReleaseReloadMetadata {
    private ReleaseReloadMetadata() {
    }

    public static void execute(ManagerService service) {
        //check status only if the server is in reloading & reload in RELOAD_STATUS_META_RELOAD
        if (ReloadManager.checkCanRelease()) {
            //try to interrupt the OBsharding-D reload
            if (!ReloadManager.interruptReload()) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Reloading finished or other client interrupt the reload");
                return;
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "OBsharding-D not in reloading or reload status not interruptible");
            return;
        }

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage(("reload release success,please reload @@metadata to make meta update").getBytes());
        ok.write(service.getConnection());
    }

}
