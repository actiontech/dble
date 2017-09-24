/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import java.util.Set;

/**
 * @author mycat
 */
public class UserConfig {

    private String name;
    private String password;
    private String encryptPassword;
    private int benchmark = 0;                        // default 0 means not check
    private UserPrivilegesConfig privilegesConfig;    //privileges for tables

    private boolean readOnly = false;
    private boolean manager = false;

    public boolean isManager() {
        return manager;
    }

    public void setManager(boolean manager) {
        this.manager = manager;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    private Set<String> schemas;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBenchmark() {
        return benchmark;
    }

    public void setBenchmark(int benchmark) {
        this.benchmark = benchmark;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setEncryptPassword(String encryptPassword) {
        this.encryptPassword = encryptPassword;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public UserPrivilegesConfig getPrivilegesConfig() {
        return privilegesConfig;
    }

    public void setPrivilegesConfig(UserPrivilegesConfig privilegesConfig) {
        this.privilegesConfig = privilegesConfig;
    }

    @Override
    public String toString() {
        return "UserConfig [name=" + this.name + ", password=" + this.password + ", encryptPassword=" +
                this.encryptPassword + ", benchmark=" + this.benchmark + ", manager=" + this.manager +
                ", readOnly=" + this.readOnly + ", schemas=" + this.schemas + "]";
    }


}
