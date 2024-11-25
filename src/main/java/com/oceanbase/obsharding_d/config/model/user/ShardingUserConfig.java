/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.user;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ShardingUserConfig extends ServerUserConfig {
    private final boolean readOnly;
    private Set<String> schemas;
    private final UserPrivilegesConfig privilegesConfig;

    public ShardingUserConfig(UserConfig user, String tenant, WallProvider blacklist, boolean readOnly, Set<String> schemas, UserPrivilegesConfig privilegesConfig) {
        super(user, tenant, blacklist);
        this.readOnly = readOnly;
        this.schemas = schemas;
        this.privilegesConfig = privilegesConfig;
    }

    public boolean isReadOnly() {
        return readOnly;
    }


    public Set<String> getSchemas() {
        return schemas;
    }


    public UserPrivilegesConfig getPrivilegesConfig() {
        return privilegesConfig;
    }

    public void changeMapToLowerCase() {
        Set<String> newSchemas = new HashSet<>();
        for (String schemaName : schemas) {
            newSchemas.add(schemaName.toLowerCase());
        }
        schemas = newSchemas;
    }

    @Override
    public void isValidSchemaInfo(UserName user, SchemaUtil.SchemaInfo schemaInfo) throws SQLException {
        if (!schemaInfo.isDual() && !SchemaUtil.MYSQL_SYS_SCHEMA.contains(schemaInfo.getSchema().toUpperCase())) {
            SchemaConfig schemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(schemaInfo.getSchema());
            if (schemaConfig == null) {
                String msg = "Table " + StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable()) + " doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
            if (!schemas.contains(schemaInfo.getSchema())) {
                String msg = "Access denied for user '" + user.getFullName() + "' to database '" + schemaInfo.getSchema() + "'";
                throw new SQLException(msg, "HY000", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            }
            schemaInfo.setSchemaConfig(schemaConfig);
        }
    }

    public boolean equalsBaseInfo(ShardingUserConfig shardingUserConfig) {
        return super.equalsBaseInfo(shardingUserConfig) &&
                this.readOnly == shardingUserConfig.isReadOnly() &&
                this.schemas.equals(shardingUserConfig.getSchemas()) &&
                isEquals(this.privilegesConfig, shardingUserConfig.getPrivilegesConfig());
    }

    private boolean isEquals(UserPrivilegesConfig o1, UserPrivilegesConfig o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1 == o2 || o1.equalsBaseInfo(o2);
    }

    @Override
    public int checkSchema(String schema) {
        if (schema == null) {
            return 0;
        }
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        if (!OBsharding_DServer.getInstance().getConfig().getSchemas().containsKey(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }
        if (schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ShardingUserConfig that = (ShardingUserConfig) o;
        return readOnly == that.readOnly &&
                Objects.equals(schemas, that.schemas) &&
                isEquals(privilegesConfig, that.privilegesConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readOnly, schemas, privilegesConfig);
    }
}
