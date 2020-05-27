package com.actiontech.dble.cluster.general.bean;

/**
 * Created by szf on 2019/3/12.
 */
public class SubscribeRequest {
    private long index;
    private int duration;
    private String path;


    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
