/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.general.response.ClusterXmlLoader;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.values.ChangeType;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.OriginClusterEvent;
import com.oceanbase.obsharding_d.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public abstract class AbstractGeneralListener<T> implements GeneralListener<T>, PathChildrenCacheListener, ClusterXmlLoader {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private ChildPathMeta<T> pathMeta;
    private int pathHeight;
    private static AtomicLong doingCount = new AtomicLong();

    public AbstractGeneralListener(ChildPathMeta<T> pathMeta) {
        this.pathMeta = pathMeta;
        this.pathHeight = ClusterLogic.forGeneral().getPathHeight(pathMeta.getPath());
    }

    @Override
    public GeneralListener<T> registerPrefixForUcore(ClusterClearKeyListener confListener) {
        confListener.addChild(this, pathMeta.getPath());
        return this;
    }

    @Override
    public final GeneralListener<T> registerPrefixForZk() {
        ZKUtils.addChildPathCache(pathMeta.getPath(), this);
        return this;
    }


    @Override
    public final void notifyProcess(OriginClusterEvent<?> changeEvent, boolean ignoreTheGrandChild) throws Exception {

        logger.info("event happen in {},type: {},data: {}", this.getClass().getSimpleName(), changeEvent.getChangeType(), changeEvent.getValue());
        //ucore may receive the grandchildren event.But zk only receive the children event. remove  grandchildren event if needed
        if (ignoreTheGrandChild && ClusterLogic.forGeneral().getPathHeight(changeEvent.getPath()) != pathHeight + 1) {
            return;
        }

        final ClusterEvent<T> newEvent;
        ClusterEvent<T> oldEvent = null;
        final ClusterValue<T> newValue = (changeEvent.getValue().convertTo(pathMeta.getChildClass()));
        final String path = changeEvent.getPath();

        switch (changeEvent.getChangeType()) {
            case ADDED:
                newEvent = new ClusterEvent<>(path, newValue, ChangeType.ADDED);
                break;
            case REMOVED:
                newEvent = new ClusterEvent<>(path, newValue, ChangeType.REMOVED);
                break;
            case UPDATE:
                /**
                 * update event are split into two event.
                 * remove the old and add the new
                 */
                final ClusterValue<T> oldValue = changeEvent.getOldValue().convertTo(pathMeta.getChildClass());
                oldEvent = new ClusterEvent<>(path, oldValue, ChangeType.REMOVED);
                oldEvent.markUpdate();
                newEvent = new ClusterEvent<>(path, newValue, ChangeType.ADDED);
                newEvent.markUpdate();
                break;
            default:
                return;
        }


        if (oldEvent != null) {
            try {
                onEvent0(oldEvent);
            } catch (Exception e) {
                logger.info("", e);
            }
        }
        try {
            onEvent0(newEvent);
        } catch (Exception e) {
            logger.info("", e);
        }
    }


    @Override
    public void notifyCluster() throws Exception {

    }

    private void onEvent0(ClusterEvent<T> newEvent) {
        boolean beginDo = false;
        try {
            if (ClusterGeneralConfig.getInstance().getClusterSender().isDetach()) {
                logger.warn("ignore event because of detached, event:{}", newEvent);
                return;
            }
            doingCount.incrementAndGet();
            beginDo = true;
            if (ClusterGeneralConfig.getInstance().getClusterSender().isDetach()) {
                logger.warn("ignore event because of detached, event:{}", newEvent);
                return;
            }
            onEvent(newEvent);
        } catch (Exception e) {
            logger.info("", e);
        } finally {
            if (beginDo) {
                doingCount.decrementAndGet();
            }
        }
    }

    private Map<String/* path */, ClusterValue<T>> keyCacheMap = new ConcurrentHashMap<>();

    @Override
    public final void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {


        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_REMOVED:
            case CHILD_UPDATED:
                break;
            default:
                return;
        }

        logger.info("event happen in {}, path: {},type: {},data: {}", this.getClass().getSimpleName(), Optional.of(event).map(PathChildrenCacheEvent::getData).map(ChildData::getPath).orElse(null), event.getType(), Optional.of(event).map(PathChildrenCacheEvent::getData).map(ChildData::getData).map(data -> new String(data)).orElse(null));
        final ChildData data = event.getData();
        if (data == null) {
            return;
        }
        if (data.getData() == null) {
            logger.warn("ignore this empty event.{}", event);
            return;
        }
        final String strValue = new String(data.getData());

        if (Strings.isEmpty(strValue)) {
            logger.warn("ignore this empty event.{}", event);
            return;
        }


        final ClusterEvent<T> newEvent;
        ClusterEvent<T> oldEvent = null;
        final ClusterValue<T> newValue = ClusterValue.readFromJson(strValue, pathMeta.getChildClass());
        final String path = data.getPath();
        switch (event.getType()) {
            case CHILD_ADDED:

                newEvent = new ClusterEvent<>(path, newValue, ChangeType.ADDED);
                keyCacheMap.put(path, newValue);
                break;
            case CHILD_REMOVED:
                newEvent = new ClusterEvent<>(path, newValue, ChangeType.REMOVED);
                keyCacheMap.remove(path);
                break;
            case CHILD_UPDATED:
                /**
                 * update event are split into two event.
                 * remove the old and add the new
                 */
                newEvent = new ClusterEvent<>(path, newValue, ChangeType.ADDED);
                newEvent.markUpdate();
                final ClusterValue<T> oldValue = keyCacheMap.get(path);
                if (oldValue == null) {
                    logger.error("miss previous message for UPDATE");
                } else {
                    oldEvent = new ClusterEvent<>(path, oldValue, ChangeType.REMOVED);
                    oldEvent.markUpdate();
                }
                keyCacheMap.put(path, newValue);
                break;
            default:
                return;
        }

        if (oldEvent != null) {
            try {
                onEvent0(oldEvent);
            } catch (Exception e) {
                logger.info("", e);
            }
        }
        try {
            onEvent0(newEvent);
        } catch (Exception e) {
            logger.info("", e);
        }

    }

    public static AtomicLong getDoingCount() {
        return doingCount;
    }
}
