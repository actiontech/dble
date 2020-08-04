package com.actiontech.dble.net.service;

import java.io.IOException;

/**
 * Created by szf on 2020/6/19.
 */
public interface AuthService {

    void register() throws IOException;

    void onConnectFailed(Throwable e);
}
