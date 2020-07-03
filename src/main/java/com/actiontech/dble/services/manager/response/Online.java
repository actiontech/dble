/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;

/**
 * @author mycat
 */
public final class Online {
    private Online() {
    }

    private static final OkPacket OK = new OkPacket();

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerService service) {
        DbleServer.getInstance().online();
        OK.write(service.getConnection());
    }

}
