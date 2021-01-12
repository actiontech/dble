package com.actiontech.dble.net.service;

import java.io.IOException;

public interface AuthService {

    void register() throws IOException;

    default void onConnectFailed(Throwable e) {
    }

}
