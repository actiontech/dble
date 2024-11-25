/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;

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
        OBsharding_DServer.getInstance().online();
        OK.write(service.getConnection());
    }

}
