package com.actiontech.dble.net.service;

import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.mysql.AuthPacket;

/**
 * Created by szf on 2020/6/19.
 */
public class AuthResultInfo {

    private String errorMsg;
    private UserConfig userConfig;

    private AuthPacket mysqlAuthPacket = null;

    public AuthResultInfo(String errorMsg, AuthPacket authPacket, UserConfig userConfig) {
        this.errorMsg = errorMsg;
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


    public UserConfig getUserConfig() {
        return userConfig;
    }

    public AuthPacket getMysqlAuthPacket() {
        return mysqlAuthPacket;
    }
}
