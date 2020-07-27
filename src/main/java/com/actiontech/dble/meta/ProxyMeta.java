package com.actiontech.dble.meta;

import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
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


    public boolean reloadMetaData(ServerConfig conf, Map<String, Set<String>> specifiedSchemas) {
        this.metaChanging = true;
        ReloadManager.metaReload();
        try {
            //back up orgin meta data
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
