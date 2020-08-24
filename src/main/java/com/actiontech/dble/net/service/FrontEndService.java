package com.actiontech.dble.net.service;

import com.actiontech.dble.config.model.user.UserName;

/**
 * Created by szf on 2020/7/7.
 */
public interface FrontEndService {

    void userConnectionCount();

    UserName getUser();

    String getExecuteSql();

    void killAndClose(String reason);
}
