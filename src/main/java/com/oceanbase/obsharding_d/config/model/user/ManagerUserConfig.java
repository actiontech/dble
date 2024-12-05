/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.user;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.sql.SQLException;
import java.util.Objects;

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
            String msg = "Unknown database '" + schemaInfo.getSchema() + "'";
            throw new SQLException(msg, "42000", ErrorCode.ER_BAD_DB_ERROR);
        }
        ManagerSchemaInfo info = ManagerSchemaInfo.getInstance();
        if (!info.getTables().containsKey(schemaInfo.getTable()) && !info.getViews().containsKey(schemaInfo.getTable())) {
            String msg = "Table " + StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable()) + " doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
    }

    @Override
    public int checkSchema(String schema) {
        if (schema != null && !ManagerSchemaInfo.SCHEMA_NAME.equals(schema.toLowerCase())) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ManagerUserConfig that = (ManagerUserConfig) o;
        return readOnly == that.readOnly;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readOnly);
    }

    public boolean equalsBaseInfo(ManagerUserConfig managerUserConfig) {
        return super.equalsBaseInfo(managerUserConfig) &&
                this.readOnly == managerUserConfig.isReadOnly();
    }
}
