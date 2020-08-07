/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.util.StringUtil;

/**
 * @author mycat
 */
public final class UseHandler {
    private UseHandler() {
    }

    public static void handle(String sql, ManagerService service, int offset) {
        String schema = sql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
            if (schema.endsWith(";")) {
                schema = schema.substring(0, schema.length() - 1);
            }
            schema = StringUtil.replaceChars(schema, "`", null);
            length = schema.length();
            if (schema.charAt(0) == '\'' && schema.charAt(length - 1) == '\'') {
                schema = schema.substring(1, length - 1);
            }
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
            }
        }
        if (schema == null || !ManagerSchemaInfo.SCHEMA_NAME.equals(schema)) {
            service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        service.setSchema(schema);
        OkPacket okPacket = new OkPacket();
        okPacket.read(OkPacket.OK);
        okPacket.write(service.getConnection());
    }

}
