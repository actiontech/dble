/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

public class ResetConnectionPacket extends MySQLPacket {
    public static final byte[] RESET = new byte[]{1, 0, 0, 0, 31};

    @Override
    public int calcPacketSize() {
        return 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Reset Connection Packet";
    }
}
