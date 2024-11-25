/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.mysql;

/**
 * @author mycat
 */
public class QuitPacket extends MySQLPacket {
    public static final byte[] QUIT = new byte[]{1, 0, 0, 0, 1};


    @Override
    public int calcPacketSize() {
        return 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Quit Packet";
    }

    @Override
    public boolean isEndOfQuery() {
        return true;
    }
}
