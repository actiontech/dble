/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;

/**
 * @author mycat
 */
public final class UseHandler {
    private UseHandler() {
    }

    public static void handle(String sql, ShardingService service, int offset) {
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
        if (!DbleServer.getInstance().getConfig().getSchemas().containsKey(schema)) {
            service.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        if (service.getUserConfig() instanceof ShardingUserConfig) {
            ShardingUserConfig userConfig = (ShardingUserConfig) (service.getUserConfig());
            if (!userConfig.getSchemas().contains(schema)) {
                String msg = "Access denied for user '" + service.getUser() + "' to database '" + schema + "'";
                service.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, msg);
                return;
            }
        }
        service.setSchema(schema);
        service.getSession2().setRowCount(0);

        service.write(service.getSession2().getOKPacket());
    }

}
