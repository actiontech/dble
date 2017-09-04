package com.actiontech.dble.backend.mysql.nio.handler.transaction;

public interface RollbackNodesHandler {

    void rollback();

    void clearResources();
}
