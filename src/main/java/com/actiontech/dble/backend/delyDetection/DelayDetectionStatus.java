package com.actiontech.dble.backend.delyDetection;

public enum DelayDetectionStatus {
    INIT(), OK(), TIMEOUT(), ERROR(), STOP();

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
