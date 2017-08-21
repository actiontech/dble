package io.mycat.config.model;

import java.util.HashMap;
import java.util.Map;


/**
 * 用户 SQL 权限配置
 *
 * @author zhuam
 */
public class UserPrivilegesConfig {

    private boolean check = false;

    private Map<String, SchemaPrivilege> schemaPrivileges = new HashMap<String, SchemaPrivilege>();

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
        SchemaPrivilege schemaPrivilege = schemaPrivileges.get(schemaName);
        return schemaPrivilege;
    }

    /**
     * 库级权限
     */
    public static class SchemaPrivilege {

        private int[] dml = new int[]{0, 0, 0, 0};

        private Map<String, TablePrivilege> tablePrivileges = new HashMap<String, TablePrivilege>();


        public int[] getDml() {
            return dml;
        }

        public void setDml(int[] dml) {
            this.dml = dml;
        }

        public void addTablePrivilege(String tableName, TablePrivilege privilege) {
            this.tablePrivileges.put(tableName, privilege);
        }

        public TablePrivilege getTablePrivilege(String tableName) {
            return tablePrivileges.get(tableName);
        }
    }

    /**
     * 表级权限
     */
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