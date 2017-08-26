package io.mycat.config.loader.zkprocess.entity.cache;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * ehcache config
 *
 *
 * author:liujun
 * Created:2016/9/19
 *
 *
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ehcache")
public class Ehcache {

    /**
     *
     */
    @XmlAttribute
    private int maxEntriesLocalHeap;

    /**
     *
     */
    @XmlAttribute
    private String maxBytesLocalDisk;

    /**
     *
     */
    @XmlAttribute
    private boolean updateCheck;

    @XmlElement
    private CacheInfo defaultCache;

    public int getMaxEntriesLocalHeap() {
        return maxEntriesLocalHeap;
    }

    public void setMaxEntriesLocalHeap(int maxEntriesLocalHeap) {
        this.maxEntriesLocalHeap = maxEntriesLocalHeap;
    }

    public String getMaxBytesLocalDisk() {
        return maxBytesLocalDisk;
    }

    public void setMaxBytesLocalDisk(String maxBytesLocalDisk) {
        this.maxBytesLocalDisk = maxBytesLocalDisk;
    }

    public boolean isUpdateCheck() {
        return updateCheck;
    }

    public void setUpdateCheck(boolean updateCheck) {
        this.updateCheck = updateCheck;
    }

    public CacheInfo getDefaultCache() {
        return defaultCache;
    }

    public void setDefaultCache(CacheInfo defaultCache) {
        this.defaultCache = defaultCache;
    }

    @Override
    public String toString() {
        String builder = "Ehcache [maxEntriesLocalHeap=" +
                maxEntriesLocalHeap +
                ", maxBytesLocalDisk=" +
                maxBytesLocalDisk +
                ", updateCheck=" +
                updateCheck +
                ", defaultCache=" +
                defaultCache +
                "]";
        return builder;
    }

}
