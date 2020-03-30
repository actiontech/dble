package com.actiontech.dble.backend.mysql.nio.handler.transaction;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, byte[] errPacket);

}
