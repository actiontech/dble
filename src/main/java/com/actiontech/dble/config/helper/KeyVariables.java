/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

public class KeyVariables {
    public static final int MARGIN_PACKET_SIZE = 1024;
    private boolean autocommit = true;
    private int isolation = -1;
    private volatile int maxPacketSize = -1;
    private boolean targetAutocommit = true;
    private int targetIsolation = -1;
    private int targetMaxPacketSize = -1;

    private boolean readOnly = false;
    private boolean lowerCase = true;

    public boolean isLowerCase() {
        return lowerCase;
    }

    public void setLowerCase(boolean lowerCase) {
        this.lowerCase = lowerCase;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public int getIsolation() {
        return isolation;
    }

    public void setIsolation(int isolation) {
        this.isolation = isolation;
    }

    public boolean isTargetAutocommit() {
        return targetAutocommit;
    }

    public void setTargetAutocommit(boolean targetAutocommit) {
        this.targetAutocommit = targetAutocommit;
    }

    public int getTargetIsolation() {
        return targetIsolation;
    }

    public void setTargetIsolation(int targetIsolation) {
        this.targetIsolation = targetIsolation;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getTargetMaxPacketSize() {
        return targetMaxPacketSize;
    }

    public void setTargetMaxPacketSize(int targetMaxPacketSize) {
        this.targetMaxPacketSize = targetMaxPacketSize;
    }

}
