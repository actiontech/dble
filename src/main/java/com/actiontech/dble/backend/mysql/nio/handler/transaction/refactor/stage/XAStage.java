package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;

public interface XAStage {

    void onEnterStage();

    XAStage next(boolean isFail);

    void onConnectionOk(MySQLConnection conn);

    // connect error
    void onConnectionError(MySQLConnection conn, int errNo);

    // connect close
    void onConnectionClose(MySQLConnection conn);

    // connect error
    void onConnectError(MySQLConnection conn);
}
