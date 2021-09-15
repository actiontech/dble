package com.actiontech.dble.singleton;

import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.util.CollectionUtil;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2019/9/16.
 */
public final class ProxyMeta {

    private static final ProxyMeta INSTANCE = new ProxyMeta();
    private volatile ProxyMetaManager tmManager;
    private volatile boolean metaChanging = false;

    private ProxyMeta() {
    }

    public static ProxyMeta getInstance() {
        return INSTANCE;
    }

    public ProxyMetaManager getTmManager() {
        while (metaChanging) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        return tmManager;
    }

    public void setTmManager(ProxyMetaManager tmManager) {
        this.tmManager = tmManager;
    }

    public boolean reloadMetaData(ServerConfig conf, Map<String, Set<String>> specifiedSchemas) {
        this.metaChanging = true;
        ReloadManager.metaReload();
        try {
            //back up origin meta data
            ProxyMetaManager tmpManager = tmManager;
            ProxyMetaManager newManager;
            if (CollectionUtil.isEmpty(specifiedSchemas)) {
                newManager = new ProxyMetaManager();
            } else {
                //if the meta just reload partly,create a deep coyp of the ProxyMetaManager as new ProxyMetaManager
                newManager = new ProxyMetaManager(tmpManager);
            }
            if (newManager.initMeta(conf, specifiedSchemas)) {
                tmManager = newManager;
                if (CollectionUtil.isEmpty(specifiedSchemas)) {
                    //deep copy do not terminate the scheduler
                    tmpManager.terminate();
                }
                return true;
            }

        } finally {
            this.metaChanging = false;
        }
        return false;
    }
}
