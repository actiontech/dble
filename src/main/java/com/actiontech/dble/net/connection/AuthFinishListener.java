package com.actiontech.dble.net.connection;


/**
 * Created by szf on 2020/6/29.
 */
public interface AuthFinishListener {

    void onCreateSuccess(BackendConnection conn);

    void onCreateFail(BackendConnection conn, Throwable e);

    void onHeartbeatSuccess(BackendConnection conn);
}
