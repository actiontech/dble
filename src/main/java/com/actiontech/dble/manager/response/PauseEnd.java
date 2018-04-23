package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;

public final class PauseEnd {
    private static final OkPacket OK = new OkPacket();

    private PauseEnd() {
    }

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerConnection c) {
        DbleServer.getInstance().getMiManager().resume();
        OK.write(c);
    }
}
