package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by szf on 2018/4/24.
 */
public class PauseInfo {

    public static final String PAUSE = "PAUSE";
    public static final String RESUME = "RESUME";

    private String from;
    private String dataNodes;

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(String dataNodes) {
        this.dataNodes = dataNodes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    private String type;

    private String split = ";";

    public PauseInfo(String from, String dataNodes, String type) {
        this.dataNodes = dataNodes;
        this.from = from;
        this.type = type;
    }

    public PauseInfo(String value) {
        String[] s = value.split(split);
        this.from = s[0];
        this.type = s[1];
        this.dataNodes = s[2];
    }

    public String toString() {
        return from + split + type + split + dataNodes;
    }
}
