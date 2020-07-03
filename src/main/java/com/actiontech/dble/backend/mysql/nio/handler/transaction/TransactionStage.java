package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.mysql.MySQLPacket;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket);

}
