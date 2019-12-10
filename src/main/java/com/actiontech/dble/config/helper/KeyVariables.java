/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

public class KeyVariables {
    private boolean lowerCase = true;
    private boolean autocommit = true;
    private int isolation = -1;
    private boolean targetAutocommit = true;
    private int targetIsolation = -1;
    private boolean readOnly = false;

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

}
