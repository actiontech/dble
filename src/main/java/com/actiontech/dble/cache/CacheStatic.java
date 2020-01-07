/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.cache;

/**
 * cache static information
 *
 * @author wuzhih
 */
public class CacheStatic {
    private long maxSize;
    private long memorySize;
    private long itemSize;
    private long accessTimes;
    private long putTimes;
    private long hitTimes;
    private long lastAccessTime;
    private long lastPutTime;

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public long getItemSize() {
        return itemSize;
    }

    public void setItemSize(long itemSize) {
        this.itemSize = itemSize;
    }

    public long getAccessTimes() {
        return accessTimes;
    }

    public void setAccessTimes(long accessTimes) {
        this.accessTimes = accessTimes;
    }

    public long getHitTimes() {
        return hitTimes;
    }

    public void setHitTimes(long hitTimes) {
        this.hitTimes = hitTimes;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getPutTimes() {
        return putTimes;
    }

    public void setPutTimes(long putTimes) {
        this.putTimes = putTimes;
    }

    public void incAccessTimes() {
        this.accessTimes++;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public void incHitTimes() {
        this.hitTimes++;
        this.accessTimes++;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public void incPutTimes() {
        this.putTimes++;
        this.lastPutTime = System.currentTimeMillis();
    }

    public long getLastPutTime() {
        return lastPutTime;
    }

    public void setLastPutTime(long lastPutTime) {
        this.lastPutTime = lastPutTime;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public void reset() {
        this.accessTimes = 0;
        this.hitTimes = 0;
        this.itemSize = 0;
        this.lastAccessTime = 0;
        this.lastPutTime = 0;
        this.memorySize = 0;
        this.putTimes = 0;

    }

    @Override
    public String toString() {
        return "CacheStatic [memorySize=" + memorySize + ", itemSize=" +
                itemSize + ", accessTimes=" + accessTimes + ", putTimes=" +
                putTimes + ", hitTimes=" + hitTimes + ", lastAccesTime=" +
                lastAccessTime + ", lastPutTime=" + lastPutTime + "]";
    }

}
