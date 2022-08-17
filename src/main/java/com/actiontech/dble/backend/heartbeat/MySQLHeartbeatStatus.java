package com.actiontech.dble.backend.heartbeat;

public enum MySQLHeartbeatStatus {
    INIT(), OK(), ERROR(), TIMEOUT(), STOP();

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
