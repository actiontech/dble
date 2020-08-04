/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.net.connection.AbstractConnection;

public class ResetConnectionPacket extends MySQLPacket {
    public static final byte[] RESET = new byte[]{1, 0, 0, 0, 31};

    @Override
    public void bufferWrite(AbstractConnection connection) {

    }

    @Override
    public int calcPacketSize() {
        return 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Reset Connection Packet";
    }
}
