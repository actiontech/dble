/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.dump;

public class ErrorMsg {
    private String target;
    private String errorMeg;

    public ErrorMsg(String target, String errorMeg) {
        this.target = target;
        this.errorMeg = errorMeg;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getErrorMeg() {
        return errorMeg;
    }

    public void setErrorMeg(String errorMeg) {
        this.errorMeg = errorMeg;
    }
}
