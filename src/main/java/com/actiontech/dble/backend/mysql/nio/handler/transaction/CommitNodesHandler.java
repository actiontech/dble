package com.actiontech.dble.backend.mysql.nio.handler.transaction;

public interface CommitNodesHandler {
    void commit();

    void clearResources();
}
