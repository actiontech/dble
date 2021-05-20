/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.DistributeLockManager;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.path.PathMeta;
import com.actiontech.dble.cluster.values.ClusterEntry;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.cluster.values.ViewChangeType;
import com.actiontech.dble.cluster.values.ViewType;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2017/10/12.
 */
public class KVStoreRepository implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreRepository.class);
    private Map<String, Map<String, String>> viewCreateSqlMap = new HashMap<>();

    private FileSystemRepository fileSystemRepository = null;

    public KVStoreRepository() {
        this.init();
        fileSystemRepository = new FileSystemRepository(viewCreateSqlMap);
        fileSystemRepository.saveMapToFile();
    }

    @Override
    public void init() {
        Map<String, Map<String, String>> map = new HashMap<>();
        try {
            List<ClusterEntry<ViewType>> allList = ClusterLogic.forView().getKVBeanOfChildPath(ChildPathMeta.of(ClusterPathUtil.getViewPath(), ViewType.class));
            for (ClusterEntry<ViewType> bean : allList) {
                String[] key = bean.getKey().split("/");
                if (bean.getKey().equals(ClusterPathUtil.getViewChangePath())) {
                    continue;
                }
                String[] value = key[key.length - 1].split(SCHEMA_VIEW_SPLIT);
                String schema = value[0];
                String viewName = value[1];
                map.computeIfAbsent(schema, k -> new ConcurrentHashMap<>());
                map.get(schema).put(viewName, bean.getValue().getData().getCreateSql());
            }

            viewCreateSqlMap = map;
            for (Map.Entry<String, SchemaConfig> schema : DbleServer.getInstance().getConfig().getSchemas().entrySet()) {
                viewCreateSqlMap.computeIfAbsent(schema.getKey(), k -> new ConcurrentHashMap<>());
            }
        } catch (Exception e) {
            LOGGER.info("init viewData from zk error :　" + e.getMessage());
        }
    }

    @Override
    public void terminate() {

    }

    @Override
    public Map<String, Map<String, String>> getViewCreateSqlMap() {
        return viewCreateSqlMap;
    }

    @Override
    public void put(String schemaName, String viewName, String createSql) {
        if (DistributeLockManager.isLooked(ClusterPathUtil.getSyncMetaLockPath())) {
            String msg = "There is another instance init meta data, try it later.";
            throw new RuntimeException(msg);
        }
        final ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.VIEW);
        DistributeLock distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getViewLockPath(schemaName, viewName),
                new ViewChangeType(SystemConfig.getInstance().getInstanceName(), UPDATE));
        final PathMeta<ViewChangeType> viewChangePath = ClusterMetaUtil.getViewChangePath(schemaName, viewName);
        if (!distributeLock.acquire()) {
            String msg = "other session/dble instance is operating view, try it later or check the cluster lock";
            LOGGER.warn(msg);
            throw new RuntimeException(msg);
        }
        try {
            ClusterDelayProvider.delayAfterGetLock();

            Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
            schemaMap.put(viewName, createSql);
            clusterHelper.setKV(ClusterMetaUtil.getViewPath(schemaName, viewName), new ViewType(createSql));

            ClusterDelayProvider.delayAfterViewSetKey();
            clusterHelper.setKV(viewChangePath, new ViewChangeType(SystemConfig.getInstance().getInstanceName(), UPDATE));
            ClusterDelayProvider.delayAfterViewNotic();

            clusterHelper.createSelfTempNode(viewChangePath.getPath(), FeedBackType.SUCCESS);
            String errorMsg = ClusterLogic.forView().waitingForAllTheNode(viewChangePath.getPath());

            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
            fileSystemRepository.saveMapToFile();

        } catch (RuntimeException e) {
            LOGGER.warn("set to cluster error : ", e);
            throw e;
        } catch (Exception e) {
            LOGGER.warn("set to cluster error : ", e);
            throw new RuntimeException(e);
        } finally {
            ClusterDelayProvider.beforeDeleteViewNotic();
            ClusterHelper.cleanPath(viewChangePath + ClusterPathUtil.SEPARATOR);
            ClusterDelayProvider.beforeReleaseViewLock();
            distributeLock.release();
        }

    }

    @Override
    public void delete(String schemaName, String viewName) {
        if (DistributeLockManager.isLooked(ClusterPathUtil.getSyncMetaLockPath())) {
            String msg = "There is another instance init meta data, try it later.";
            throw new RuntimeException(msg);
        }
        final ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.VIEW);
        DistributeLock distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getViewLockPath(schemaName, viewName),
                new ViewChangeType(SystemConfig.getInstance().getInstanceName(), DELETE));
        final PathMeta<ViewChangeType> viewChangePath = ClusterMetaUtil.getViewChangePath(schemaName, viewName);
        if (!distributeLock.acquire()) {
            String msg = "other session/dble instance is operating view, try it later or check the cluster lock";
            LOGGER.warn(msg);
            throw new RuntimeException(msg);
        }
        try {
            viewCreateSqlMap.get(schemaName).remove(viewName);
            ClusterDelayProvider.delayAfterGetLock();
            ClusterHelper.cleanKV(ClusterPathUtil.getViewPath(schemaName, viewName));
            ClusterDelayProvider.delayAfterViewSetKey();
            clusterHelper.setKV(viewChangePath, new ViewChangeType(SystemConfig.getInstance().getInstanceName(), DELETE));
            ClusterDelayProvider.delayAfterViewNotic();
            clusterHelper.createSelfTempNode(viewChangePath.getPath(), FeedBackType.SUCCESS);
            String errorMsg = ClusterLogic.forView().waitingForAllTheNode(viewChangePath.getPath());

            if (errorMsg != null) {
                throw new RuntimeException(errorMsg);
            }
            fileSystemRepository.saveMapToFile();
        } catch (RuntimeException e) {
            LOGGER.warn("delete view node error :　" + e.getMessage());
            throw e;
        } catch (Exception e) {
            LOGGER.warn("delete view node error :　" + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            ClusterDelayProvider.beforeDeleteViewNotic();
            ClusterHelper.cleanPath(viewChangePath + ClusterPathUtil.SEPARATOR);
            ClusterDelayProvider.beforeReleaseViewLock();
            distributeLock.release();
        }
    }
}
