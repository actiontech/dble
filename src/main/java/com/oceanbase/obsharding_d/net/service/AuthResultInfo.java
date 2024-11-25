/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.net.mysql.AuthPacket;

/**
 * Created by szf on 2020/6/19.
 */
public class AuthResultInfo {

    private String errorMsg;
    private UserName user;
    private UserConfig userConfig;

    private AuthPacket mysqlAuthPacket = null;

    public AuthResultInfo(String errorMsg, AuthPacket authPacket, UserName user, UserConfig userConfig) {
        this.errorMsg = errorMsg;
        this.user = user;
        this.userConfig = userConfig;
        this.mysqlAuthPacket = authPacket;
    }

    public AuthResultInfo(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public boolean isSuccess() {
        return errorMsg == null;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public UserName getUser() {
        return user;
    }

    public void setUser(UserName user) {
        this.user = user;
    }

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public AuthPacket getMysqlAuthPacket() {
        return mysqlAuthPacket;
    }
}
