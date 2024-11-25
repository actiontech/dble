/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.user;

import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.util.SplitUtil;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class UserConfig {
    private int id;
    protected final String name;
    protected final String password;
    protected final boolean isEncrypt;
    protected final Set<String> whiteIPs;
    protected final int maxCon;

    public UserConfig(UserConfig user) {
        this.name = user.name;
        this.password = user.password;
        this.isEncrypt = user.isEncrypt;
        this.whiteIPs = user.whiteIPs;
        this.maxCon = user.maxCon;
    }

    public UserConfig(String name, String password, boolean isEncrypt, String strWhiteIPs, String strMaxCon) {
        this.name = name;
        this.password = password;
        this.isEncrypt = isEncrypt;
        this.whiteIPs = genWhiteIPs(strWhiteIPs);

        int maxConn = 0;
        if (!StringUtil.isEmpty(strMaxCon)) {
            maxConn = Integer.parseInt(strMaxCon);
            if (maxConn < 0) {
                maxConn = 0;
            }
        }
        this.maxCon = maxConn;
    }

    private Set<String> genWhiteIPs(String strWhiteIPs) {
        Set<String> result = new HashSet<>();
        if (!StringUtil.isEmpty(strWhiteIPs)) {
            String[] theWhiteIPs = SplitUtil.split(strWhiteIPs, ',');
            result.addAll(Arrays.asList(theWhiteIPs));
        }
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    public boolean isEncrypt() {
        return isEncrypt;
    }

    public Set<String> getWhiteIPs() {
        return whiteIPs;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void isValidSchemaInfo(UserName user, SchemaUtil.SchemaInfo schemaInfo) throws SQLException {
    }

    public int checkSchema(String schema) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserConfig that = (UserConfig) o;
        return isEncrypt == that.isEncrypt &&
                maxCon == that.maxCon &&
                Objects.equals(name, that.name) &&
                Objects.equals(password, that.password) &&
                Objects.equals(whiteIPs, that.whiteIPs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, password, isEncrypt, whiteIPs, maxCon);
    }

    public boolean equalsBaseInfo(UserConfig userConfig) {
        return StringUtil.equalsWithEmpty(this.name, userConfig.getName()) &&
                StringUtil.equalsWithEmpty(this.password, userConfig.getPassword()) &&
                this.isEncrypt == userConfig.isEncrypt() &&
                this.maxCon == userConfig.getMaxCon() &&
                this.whiteIPs.equals(userConfig.getWhiteIPs());
    }
}
