/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

public class ChangeItem {
    private ChangeType type;
    private Object item;
    private ChangeItemType itemType;
    private boolean affectHeartbeat;
    private boolean affectDelayDetection;
    private boolean affectConnectionPool;
    private boolean affectTestConn;
    private boolean affectEntryDbGroup;
    //connection pool capacity
    private boolean affectPoolCapacity;

    public ChangeItem(ChangeType type, Object item, ChangeItemType itemType) {
        this.type = type;
        this.item = item;
        this.itemType = itemType;
    }

    public ChangeItemType getItemType() {
        return itemType;
    }

    public boolean isAffectHeartbeat() {
        return affectHeartbeat;
    }

    public void setAffectHeartbeat(boolean affectHeartbeat) {
        this.affectHeartbeat = affectHeartbeat;
    }

    public boolean isAffectConnectionPool() {
        return affectConnectionPool;
    }

    public void setAffectConnectionPool(boolean affectConnectionPool) {
        this.affectConnectionPool = affectConnectionPool;
    }

    public boolean isAffectPoolCapacity() {
        return affectPoolCapacity;
    }

    public void setAffectPoolCapacity(boolean affectPoolCapacity) {
        this.affectPoolCapacity = affectPoolCapacity;
    }

    public boolean isAffectTestConn() {
        return affectTestConn;
    }

    public void setAffectTestConn(boolean affectTestConn) {
        this.affectTestConn = affectTestConn;
    }

    public boolean isAffectEntryDbGroup() {
        return affectEntryDbGroup;
    }

    public void setAffectEntryDbGroup(boolean affectEntryDbGroup) {
        this.affectEntryDbGroup = affectEntryDbGroup;
    }

    public ChangeType getType() {
        return type;
    }

    public void setType(ChangeType type) {
        this.type = type;
    }

    public Object getItem() {
        return item;
    }

    public void setItem(Object item) {
        this.item = item;
    }

    public boolean isAffectDelayDetection() {
        return affectDelayDetection;
    }

    public void setAffectDelayDetection(boolean affectDelayDetection) {
        this.affectDelayDetection = affectDelayDetection;
    }

    @Override
    public String toString() {
        return "ChangeItem{" +
                "type=" + type +
                ", item=" + item +
                ", affectHeartbeat=" + affectHeartbeat +
                ", affectConnectionPool=" + affectConnectionPool +
                ", affectTestConn=" + affectTestConn +
                ", affectEntryDbGroup=" + affectEntryDbGroup +
                ", affectPoolCapacity=" + affectPoolCapacity +
                ", affectDelayDetection=" + affectDelayDetection +
                '}';
    }
}
