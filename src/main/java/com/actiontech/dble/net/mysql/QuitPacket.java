/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

/**
 * @author mycat
 */
public class QuitPacket extends MySQLPacket {
    public static final byte[] QUIT = new byte[]{1, 0, 0, 0, 1};
    public static final byte[] GETPUBLICKEY = new byte[]{1, 0, 0, 3, 2};


    @Override
    public int calcPacketSize() {
        return 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Quit Packet";
    }

}
