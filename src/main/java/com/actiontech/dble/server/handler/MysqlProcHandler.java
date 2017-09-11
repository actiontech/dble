/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;


import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.server.ServerConnection;

public final class MysqlProcHandler {
    private MysqlProcHandler() {
    }

    private static final int FIELD_COUNT = 2;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("name",
                Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("type", Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(ServerConnection c) {
        MysqlInformationSchemaHandler.doWrite(FIELD_COUNT, FIELDS, c);
    }


}
