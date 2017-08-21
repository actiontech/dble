package io.mycat.backend.mysql.nio.handler.transaction;

public interface RollbackNodesHandler {

    void rollback();

    void clearResources();
}