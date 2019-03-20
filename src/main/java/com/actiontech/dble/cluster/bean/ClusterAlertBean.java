package com.actiontech.dble.cluster.bean;

import java.util.Map;

/**
 * Created by szf on 2019/3/11.
 */
public class ClusterAlertBean {
    String code;
    String level;
    String desc;
    String sourceComponentType;
    String sourceComponentId;
    String alertComponentType;
    String alertComponentId;
    String serverId;
    long timestampUnix;
    long resolveTimestampUnix;

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    Map<String, String> labels;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getSourceComponentType() {
        return sourceComponentType;
    }

    public void setSourceComponentType(String sourceComponentType) {
        this.sourceComponentType = sourceComponentType;
    }

    public String getSourceComponentId() {
        return sourceComponentId;
    }

    public void setSourceComponentId(String sourceComponentId) {
        this.sourceComponentId = sourceComponentId;
    }

    public String getAlertComponentType() {
        return alertComponentType;
    }

    public void setAlertComponentType(String alertComponentType) {
        this.alertComponentType = alertComponentType;
    }

    public String getAlertComponentId() {
        return alertComponentId;
    }

    public void setAlertComponentId(String alertComponentId) {
        this.alertComponentId = alertComponentId;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public long getTimestampUnix() {
        return timestampUnix;
    }

    public void setTimestampUnix(long timestampUnix) {
        this.timestampUnix = timestampUnix;
    }

    public long getResolveTimestampUnix() {
        return resolveTimestampUnix;
    }

    public void setResolveTimestampUnix(long resolveTimestampUnix) {
        this.resolveTimestampUnix = resolveTimestampUnix;
    }
}
