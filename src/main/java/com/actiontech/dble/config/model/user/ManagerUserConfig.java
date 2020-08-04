/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.util.StringUtil;

import java.sql.SQLException;

public class ManagerUserConfig extends UserConfig {
    private final boolean readOnly;

    public ManagerUserConfig(UserConfig user, boolean readOnly) {
        super(user);
        if (whiteIPs.size() > 0) {
            whiteIPs.add("127.0.0.1");
            whiteIPs.add("0:0:0:0:0:0:0:1");
        }
        this.readOnly = readOnly;
    }


    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void isValidSchemaInfo(UserName user, SchemaUtil.SchemaInfo schemaInfo) throws SQLException {
        schemaInfo.setSchema((schemaInfo.getSchema().toLowerCase()));
        schemaInfo.setTable(schemaInfo.getTable().toLowerCase());
        if (!ManagerSchemaInfo.SCHEMA_NAME.equals(schemaInfo.getSchema())) {
            String msg = "Access denied for user '" + user + "' to database '" + schemaInfo.getSchema() + "'";
            throw new SQLException(msg, "HY000", ErrorCode.ER_DBACCESS_DENIED_ERROR);
        }
        if (!ManagerSchemaInfo.getInstance().getTables().containsKey(schemaInfo.getTable())) {
            String msg = "Table " + StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable()) + " doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
    }
}
