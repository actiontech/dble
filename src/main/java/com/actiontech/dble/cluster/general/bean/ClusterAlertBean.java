package com.actiontech.dble.cluster.general.bean;

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

    public ClusterAlertBean setLabels(Map<String, String> xlabels) {
        this.labels = xlabels;
        return this;
    }

    Map<String, String> labels;

    public String getCode() {
        return code;
    }

    public ClusterAlertBean setCode(String xcode) {
        this.code = xcode;
        return this;
    }

    public String getLevel() {
        return level;
    }

    public ClusterAlertBean setLevel(String xlevel) {
        this.level = xlevel;
        return this;
    }

    public String getDesc() {
        return desc;
    }

    public ClusterAlertBean setDesc(String xdesc) {
        this.desc = xdesc;
        return this;
    }

    public String getSourceComponentType() {
        return sourceComponentType;
    }

    public ClusterAlertBean setSourceComponentType(String xsourceComponentType) {
        this.sourceComponentType = xsourceComponentType;
        return this;
    }

    public String getSourceComponentId() {
        return sourceComponentId;
    }

    public ClusterAlertBean setSourceComponentId(String xsourceComponentId) {
        this.sourceComponentId = xsourceComponentId;
        return this;
    }

    public String getAlertComponentType() {
        return alertComponentType;
    }

    public ClusterAlertBean setAlertComponentType(String xalertComponentType) {
        this.alertComponentType = xalertComponentType;
        return this;
    }

    public String getAlertComponentId() {
        return alertComponentId;
    }

    public ClusterAlertBean setAlertComponentId(String xalertComponentId) {
        this.alertComponentId = xalertComponentId;
        return this;
    }

    public String getServerId() {
        return serverId;
    }

    public ClusterAlertBean setServerId(String xserverId) {
        this.serverId = xserverId;
        return this;
    }

    public long getTimestampUnix() {
        return timestampUnix;
    }

    public ClusterAlertBean setTimestampUnix(long xtimestampUnix) {
        this.timestampUnix = xtimestampUnix;
        return this;
    }

    public long getResolveTimestampUnix() {
        return resolveTimestampUnix;
    }

    public ClusterAlertBean setResolveTimestampUnix(long xresolveTimestampUnix) {
        this.resolveTimestampUnix = xresolveTimestampUnix;
        return this;
    }
}
