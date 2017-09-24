/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

/**
 * @author EmptyPacket
 */
public class EmptyPacket extends MySQLPacket {
    public static final byte[] EMPTY = new byte[]{0, 0, 0, 3};

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Empty Packet";
    }

}
