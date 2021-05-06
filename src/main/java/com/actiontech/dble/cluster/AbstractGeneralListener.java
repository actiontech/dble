/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.general.response.ClusterXmlLoader;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.values.ChangeType;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Optional;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public abstract class AbstractGeneralListener<T> implements GeneralListener<T>, PathChildrenCacheListener, ClusterXmlLoader {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private ChildPathMeta<T> pathMeta;
    private int pathHeight;

    public AbstractGeneralListener(ChildPathMeta<T> pathMeta) {
        this.pathMeta = pathMeta;
        this.pathHeight = ClusterLogic.forGeneral().getPathHeight(pathMeta.getPath());
    }

    @Override
    public final GeneralListener<T> registerPrefixForUcore(ClusterClearKeyListener confListener) {
        confListener.addChild(this, pathMeta.getPath());
        return this;
    }

    @Override
    public final GeneralListener<T> registerPrefixForZk() {
        ZKUtils.addChildPathCache(pathMeta.getPath(), this);
        return this;
    }


    @Override
    public final void notifyProcess(ClusterEvent<?> configValue, boolean ignoreTheGrandChild) throws Exception {

        logger.info("event happen in {}, path: {},type: {},data: {}", this.getClass().getSimpleName(), configValue.getPath(), configValue.getChangeType(), configValue.getValue());
        //ucore may receive the grandchildren event.But zk only receive the children event.
        if (ignoreTheGrandChild && ClusterLogic.forGeneral().getPathHeight(configValue.getPath()) != pathHeight + 1) {
            return;
        }

        onEvent(new ClusterEvent<>(configValue.getPath(), (configValue.getValue().convertTo(pathMeta.getChildClass())), configValue.getChangeType()));
    }


    @Override
    public void notifyCluster() throws Exception {

    }

    @Override
    public final void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

        final ChangeType type;
        switch (event.getType()) {
            case CHILD_ADDED:
                type = ChangeType.ADDED;
                break;
            case CHILD_REMOVED:
                type = ChangeType.REMOVED;
                break;
            case CHILD_UPDATED:
                //noinspection deprecation
                type = ChangeType.UPDATED;
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
        onEvent(new ClusterEvent<>(data.getPath(), ClusterValue.readFromJson(strValue, pathMeta.getChildClass()), type));
    }
}
