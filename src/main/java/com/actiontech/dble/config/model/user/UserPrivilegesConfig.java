/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * UserPrivilegesConfig
 *
 * @author zhuam
 */
public class UserPrivilegesConfig {

    private boolean check = false;

    private volatile Map<String, SchemaPrivilege> schemaPrivileges = new HashMap<>();

    public boolean isCheck() {
        return check;
    }

    public void setCheck(boolean check) {
        this.check = check;
    }

    public void addSchemaPrivilege(String schemaName, SchemaPrivilege privilege) {
        this.schemaPrivileges.put(schemaName, privilege);
    }

    public SchemaPrivilege getSchemaPrivilege(String schemaName) {
        return schemaPrivileges.get(schemaName);
    }

    public Map<String, SchemaPrivilege> getSchemaPrivileges() {
        return schemaPrivileges;
    }

    public void changeMapToLowerCase() {
        Map<String, SchemaPrivilege> newSchemaPrivileges = new HashMap<>();

        for (Map.Entry<String, SchemaPrivilege> entry : schemaPrivileges.entrySet()) {
            entry.getValue().changeMapToLowerCase();
            newSchemaPrivileges.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        schemaPrivileges = newSchemaPrivileges;
    }

    public static class SchemaPrivilege {

        private int[] dml = new int[]{0, 0, 0, 0};

        private Map<String, TablePrivilege> tablePrivileges = new HashMap<>();


        public int[] getDml() {
            return dml;
        }

        public void setDml(int[] dml) {
            this.dml = dml;
        }

        public void addTablePrivilege(String tableName, TablePrivilege privilege) {
            this.tablePrivileges.put(tableName, privilege);
        }

        public Map<String, TablePrivilege> getTablePrivileges() {
            return tablePrivileges;
        }

        public void changeMapToLowerCase() {
            Map<String, TablePrivilege> newTablePrivileges = new HashMap<>();
            for (Map.Entry<String, TablePrivilege> entry : tablePrivileges.entrySet()) {
                newTablePrivileges.put(entry.getKey().toLowerCase(), entry.getValue());
            }
            tablePrivileges = newTablePrivileges;
        }

        public TablePrivilege getTablePrivilege(String tableName) {
            return tablePrivileges.get(tableName);
        }

        public Set<String> getTables() {
            return tablePrivileges.keySet();
        }
    }

    public static class TablePrivilege {
        private int[] dml = new int[]{0, 0, 0, 0};

        public int[] getDml() {
            return dml;
        }

        public void setDml(int[] dml) {
            this.dml = dml;
        }
    }
}
