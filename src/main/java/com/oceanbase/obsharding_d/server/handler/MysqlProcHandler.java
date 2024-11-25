/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.handler;


import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

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

    public static void handle(ShardingService service) {
        MysqlSystemSchemaHandler.doWrite(FIELD_COUNT, FIELDS, null, service);
    }


}
