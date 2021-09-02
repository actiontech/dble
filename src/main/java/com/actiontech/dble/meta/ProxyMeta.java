package com.actiontech.dble.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2019/9/16.
 */
public final class ProxyMeta {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyMeta.class);
    private static final ProxyMeta INSTANCE = new ProxyMeta();

    private volatile ProxyMetaManager tmManager;

    private volatile boolean metaChanging = false;

    private ProxyMeta() {

    }

    public static ProxyMeta getInstance() {
        return INSTANCE;
    }

    public void setTmManager(ProxyMetaManager tmManager) {
        this.tmManager = tmManager;
    }


    public ProxyMetaManager getTmManager() {
        while (metaChanging) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        return tmManager;
    }
}
