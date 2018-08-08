package com.actiontech.dble.config;

/**
 * Created by szf on 2018/8/6.
 */
public class ErrorInfo {
    private String type;
    private String detail;
    private String level;

    public ErrorInfo(String type, String level, String detail) {
        this.type = type;
        this.level = level;
        this.detail = detail;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

}
