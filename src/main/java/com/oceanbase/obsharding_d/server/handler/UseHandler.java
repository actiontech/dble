/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;

/**
 * @author mycat
 */
public final class UseHandler {
    private UseHandler() {
    }

    public static void handle(String sql, ShardingService service, int offset) {
        String schema = getSchemaName(sql, offset);
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        if (!OBsharding_DServer.getInstance().getConfig().getSchemas().containsKey(schema)) {
            service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        ShardingUserConfig userConfig = service.getUserConfig();
        if (!userConfig.getSchemas().contains(schema)) {
            String msg = "Access denied for user '" + service.getUser().getFullName() + "' to database '" + schema + "'";
            service.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, msg);
            return;
        }
        service.setSchema(schema);
        service.getSession2().setRowCount(0);

        service.writeOkPacket();
    }

    public static String getSchemaName(String sql, int offset) {
        String schema = sql.substring(offset).trim();
        if (schema.length() == 0) {
            return "";
        }
        if (schema.endsWith(";")) {
            schema = schema.substring(0, schema.length() - 1);
        }
        return StringUtil.removeApostropheOrBackQuote(schema);
    }

}
