/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.privileges;

import com.actiontech.dble.config.model.user.ServerUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserPrivilegesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingPrivileges {
    private static ShardingPrivileges instance = new ShardingPrivileges();

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardingPrivileges.class);

    public static ShardingPrivileges instance() {
        return instance;
    }

    protected ShardingPrivileges() {

    }



    public enum CheckType {
        INSERT, UPDATE, SELECT, DELETE
    }

    // check SQL Privilege
    public static boolean checkPrivilege(ServerUserConfig userConfig, String schema, String tableName, CheckType chekcType) {
        if (!(userConfig instanceof ShardingUserConfig)) {
            return true;
        }
        UserPrivilegesConfig userPrivilege = ((ShardingUserConfig) userConfig).getPrivilegesConfig();
        if (userPrivilege == null || !userPrivilege.isCheck()) {
            return true;
        }
        UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = userPrivilege.getSchemaPrivilege(schema);
        if (schemaPrivilege == null) {
            return true;
        }
        UserPrivilegesConfig.TablePrivilege tablePrivilege = schemaPrivilege.getTablePrivilege(tableName);
        if (tablePrivilege == null && schemaPrivilege.getDml().length == 0) {
            return true;
        }
        int index = -1;
        if (chekcType == CheckType.INSERT) {
            index = 0;
        } else if (chekcType == CheckType.UPDATE) {
            index = 1;
        } else if (chekcType == CheckType.SELECT) {
            index = 2;
        } else if (chekcType == CheckType.DELETE) {
            index = 3;
        }
        if (tablePrivilege != null) {
            return tablePrivilege.getDml()[index] > 0;
        } else {
            return schemaPrivilege.getDml()[index] > 0;
        }
    }
}
