/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.util.KVPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/23.
 */
public class CKVStoreRepository implements Repository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CKVStoreRepository.class);

    private Map<String, Map<String, String>> viewCreateSqlMap = new HashMap<String, Map<String, String>>();

    FileSystemRepository fileSystemRepository = null;

    @Override
    public Map<String, Map<String, String>> getViewCreateSqlMap() {
        return viewCreateSqlMap;
    }


    public CKVStoreRepository() {
        init();
        fileSystemRepository = new FileSystemRepository(viewCreateSqlMap);
        fileSystemRepository.saveUcoreMap();
    }

    @Override
    public void init() {
        List<KvBean> allList = ClusterHelper.getKVPath(ClusterPathUtil.getViewPath());
        for (KvBean bean : allList) {
            String[] key = bean.getKey().split("/");
            if (key.length == 5) {
                String[] value = key[key.length - 1].split(SCHEMA_VIEW_SPLIT);
                if (viewCreateSqlMap.get(value[0]) == null) {
                    Map<String, String> schemaMap = new ConcurrentHashMap<String, String>();
                    viewCreateSqlMap.put(value[0], schemaMap);
                }
                viewCreateSqlMap.get(value[0]).put(value[1], bean.getValue());
            }
        }
        for (Map.Entry<String, SchemaConfig> schema : DbleServer.getInstance().getConfig().getSchemas().entrySet()) {
            if (viewCreateSqlMap.get(schema.getKey()) == null) {
                viewCreateSqlMap.put(schema.getKey(), new ConcurrentHashMap<String, String>());
            }
        }

    }

    @Override
    public void terminate() {

    }


    @Override
    public void put(String schemaName, String viewName, String createSql) {
        Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);

        StringBuffer sb = new StringBuffer().append(ClusterPathUtil.getViewPath()).
                append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        StringBuffer lsb = new StringBuffer().append(ClusterPathUtil.getViewPath()).
                append(SEPARATOR).append(LOCK).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        StringBuffer nsb = new StringBuffer().append(ClusterPathUtil.getViewPath()).
                append(SEPARATOR).append(UPDATE).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        DistributeLock distributeLock = new DistributeLock(lsb.toString(),
                SystemConfig.getInstance().getInstanceId() + SCHEMA_VIEW_SPLIT + UPDATE);


        try {
            int time = 0;
            while (!distributeLock.acquire()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                if (time++ % 10 == 0) {
                    LOGGER.info(" view meta waiting for the lock " + schemaName + " " + viewName);
                }
            }

            ClusterDelayProvider.delayAfterGetLock();
            schemaMap.put(viewName, createSql);
            ClusterHelper.setKV(sb.toString(), createSql);
            ClusterDelayProvider.delayAfterViewSetKey();
            ClusterHelper.setKV(nsb.toString(), SystemConfig.getInstance().getInstanceId() + SCHEMA_VIEW_SPLIT + UPDATE);
            ClusterDelayProvider.delayAfterViewNotic();
            //self reponse set
            ClusterHelper.setKV(nsb.toString() + ClusterPathUtil.SEPARATOR + SystemConfig.getInstance().getInstanceId(), ClusterPathUtil.SUCCESS);

            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, nsb.toString() + SEPARATOR);

            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
            fileSystemRepository.saveUcoreMap();

        } catch (RuntimeException e) {
            LOGGER.warn("set to ucore node error :　" + e.getMessage());
            throw e;
        } catch (Exception e) {
            LOGGER.warn("set to ucore node error :　" + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            ClusterDelayProvider.beforeDeleteViewNotic();
            ClusterHelper.cleanPath(nsb.toString() + SEPARATOR);
            ClusterDelayProvider.beforeReleaseViewLock();
            distributeLock.release();
        }

    }


    /***
     * delete ucore K/V view meta
     * try get the view meta lock & delete the view meta
     * then create a delete view node ,wait for all the online node response
     * check all the response data send the alarm if not success
     * @param schemaName
     * @param view
     */
    @Override
    public void delete(String schemaName, String view) {
        StringBuilder sb = new StringBuilder().append(ClusterPathUtil.getViewPath()).
                append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(view);
        StringBuilder nsb = new StringBuilder().append(ClusterPathUtil.getViewPath()).
                append(SEPARATOR).append(UPDATE).append(SEPARATOR).
                append(schemaName).append(SCHEMA_VIEW_SPLIT).append(view);
        String lsb = ClusterPathUtil.getViewPath() +
                SEPARATOR + LOCK + SEPARATOR + schemaName + SCHEMA_VIEW_SPLIT + view;
        DistributeLock distributeLock = new DistributeLock(lsb,
                SystemConfig.getInstance().getInstanceId() + SCHEMA_VIEW_SPLIT + DELETE);

        try {
            viewCreateSqlMap.get(schemaName).remove(view);
            int time = 0;
            while (!distributeLock.acquire()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                if (time++ % 10 == 0) {
                    LOGGER.warn(" view meta waiting for the lock " + schemaName + " " + view);
                }
            }
            ClusterDelayProvider.delayAfterGetLock();
            ClusterHelper.cleanKV(sb.toString());
            ClusterDelayProvider.delayAfterViewSetKey();
            ClusterHelper.setKV(nsb.toString(), SystemConfig.getInstance().getInstanceId() + SCHEMA_VIEW_SPLIT + DELETE);
            ClusterDelayProvider.delayAfterViewNotic();

            //self reponse set
            ClusterHelper.setKV(nsb.toString() + ClusterPathUtil.SEPARATOR + SystemConfig.getInstance().getInstanceId(), ClusterPathUtil.SUCCESS);

            String errorMsg = ClusterHelper.waitingForAllTheNode(ClusterPathUtil.SUCCESS, nsb.toString() + SEPARATOR);

            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
            fileSystemRepository.saveUcoreMap();
        } catch (RuntimeException e) {
            LOGGER.warn("delete ucore node error :　" + e.getMessage());
            throw e;
        } catch (Exception e) {
            LOGGER.warn("delete ucore node error :　" + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            ClusterDelayProvider.beforeDeleteViewNotic();
            ClusterHelper.cleanPath(nsb.toString() + SEPARATOR);
            ClusterDelayProvider.beforeReleaseViewLock();
            distributeLock.release();
        }
    }


}
