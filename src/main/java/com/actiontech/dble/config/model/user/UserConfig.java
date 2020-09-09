/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class UserConfig {
    private int id;
    protected final String name;
    protected final String password;
    protected final Set<String> whiteIPs;
    protected final int maxCon;
    protected final boolean usingDecrypt;

    public UserConfig(UserConfig user) {
        this.name = user.name;
        this.password = user.password;
        this.whiteIPs = user.whiteIPs;
        this.maxCon = user.maxCon;
        this.usingDecrypt = user.usingDecrypt;
    }

    public UserConfig(String name, String password, String strWhiteIPs, String strMaxCon, boolean usingDecrypt) {
        this.name = name;
        this.password = password;
        this.whiteIPs = genWhiteIPs(strWhiteIPs);

        int maxConn = -1;
        if (!StringUtil.isEmpty(strMaxCon)) {
            maxConn = Integer.parseInt(strMaxCon);
            if (maxConn < 0) {
                maxConn = -1;
            }
        }
        this.maxCon = maxConn;
        this.usingDecrypt = usingDecrypt;
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

    public Set<String> getWhiteIPs() {
        return whiteIPs;
    }


    public int getMaxCon() {
        return maxCon;
    }

    public boolean isUsingDecrypt() {
        return usingDecrypt;
    }

    public void isValidSchemaInfo(UserName user, SchemaUtil.SchemaInfo schemaInfo) throws SQLException {
    }
}
