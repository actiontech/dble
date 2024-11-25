/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.util.StringUtil;

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
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
            }
        }
        if (!ManagerSchemaInfo.SCHEMA_NAME.equals(schema)) {
            service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        service.setSchema(schema);
        OkPacket okPacket = OkPacket.getDefault();
        okPacket.write(service.getConnection());
    }

}
