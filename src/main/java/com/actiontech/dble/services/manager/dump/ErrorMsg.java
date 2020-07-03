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
