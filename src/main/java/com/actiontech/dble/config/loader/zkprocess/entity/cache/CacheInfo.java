/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.cache;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * author:liujun
 * Created:2016/9/19
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement(name = "defaultCache")
public class CacheInfo {

    @XmlAttribute
    private int maxElementsInMemory;

    /**
     * eternal:if true ,ignore the timeout
     */
    @XmlAttribute
    private boolean eternal;

    /**
     * overflowToDisk
     */
    @XmlAttribute
    private boolean overflowToDisk;

    /**
     * diskSpoolBufferSizeMB:for DiskStore .the default is 30MB.
     */
    @XmlAttribute
    private int diskSpoolBufferSizeMB;

    /**
     * maxElementsOnDisk:
     */
    @XmlAttribute
    private int maxElementsOnDisk;

    /**
     * diskPersistent
     */
    @XmlAttribute
    private boolean diskPersistent;

    /**
     * diskExpiryThreadIntervalSeconds
     */
    @XmlAttribute
    private int diskExpiryThreadIntervalSeconds;

    /**
     * memoryStoreEvictionPolicy:
     * when reached maxElementsInMemory,
     * Ehcache will clean the memory.the default is LRU.
     * Other Policy is FIFO Or LFU.
     */
    @XmlAttribute
    private String memoryStoreEvictionPolicy;

    public int getMaxElementsInMemory() {
        return maxElementsInMemory;
    }

    public void setMaxElementsInMemory(int maxElementsInMemory) {
        this.maxElementsInMemory = maxElementsInMemory;
    }

    public boolean isEternal() {
        return eternal;
    }

    public void setEternal(boolean eternal) {
        this.eternal = eternal;
    }

    public boolean isOverflowToDisk() {
        return overflowToDisk;
    }

    public void setOverflowToDisk(boolean overflowToDisk) {
        this.overflowToDisk = overflowToDisk;
    }

    public int getDiskSpoolBufferSizeMB() {
        return diskSpoolBufferSizeMB;
    }

    public void setDiskSpoolBufferSizeMB(int diskSpoolBufferSizeMB) {
        this.diskSpoolBufferSizeMB = diskSpoolBufferSizeMB;
    }

    public int getMaxElementsOnDisk() {
        return maxElementsOnDisk;
    }

    public void setMaxElementsOnDisk(int maxElementsOnDisk) {
        this.maxElementsOnDisk = maxElementsOnDisk;
    }

    public boolean isDiskPersistent() {
        return diskPersistent;
    }

    public void setDiskPersistent(boolean diskPersistent) {
        this.diskPersistent = diskPersistent;
    }

    public int getDiskExpiryThreadIntervalSeconds() {
        return diskExpiryThreadIntervalSeconds;
    }

    public void setDiskExpiryThreadIntervalSeconds(int diskExpiryThreadIntervalSeconds) {
        this.diskExpiryThreadIntervalSeconds = diskExpiryThreadIntervalSeconds;
    }

    public String getMemoryStoreEvictionPolicy() {
        return memoryStoreEvictionPolicy;
    }

    public void setMemoryStoreEvictionPolicy(String memoryStoreEvictionPolicy) {
        this.memoryStoreEvictionPolicy = memoryStoreEvictionPolicy;
    }

    @Override
    public String toString() {
        String builder = "CacheInfo [maxElementsInMemory=" +
                maxElementsInMemory +
                ", eternal=" +
                eternal +
                ", overflowToDisk=" +
                overflowToDisk +
                ", diskSpoolBufferSizeMB=" +
                diskSpoolBufferSizeMB +
                ", maxElementsOnDisk=" +
                maxElementsOnDisk +
                ", diskPersistent=" +
                diskPersistent +
                ", diskExpiryThreadIntervalSeconds=" +
                diskExpiryThreadIntervalSeconds +
                ", memoryStoreEvictionPolicy=" +
                memoryStoreEvictionPolicy +
                "]";
        return builder;
    }

}
