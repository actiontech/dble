/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

import java.sql.SQLException;
import java.util.HashSet;
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
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaInfo.getSchema());
            if (schemaConfig == null) {
                String msg = "Table " + StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable()) + " doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
            if (!schemas.contains(schemaInfo.getSchema())) {
                String msg = "Access denied for user '" + user + "' to database '" + schemaInfo.getSchema() + "'";
                throw new SQLException(msg, "HY000", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            }
            schemaInfo.setSchemaConfig(schemaConfig);
        }
    }
}
