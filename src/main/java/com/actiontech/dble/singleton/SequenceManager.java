/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.route.sequence.handler.*;
import com.actiontech.dble.services.FrontendService;
import com.google.common.collect.Sets;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.sql.SQLNonTransientException;
import java.util.Set;

/**
 * Created by szf on 2019/9/19.
 */
public final class SequenceManager {
    private static final SequenceManager INSTANCE = new SequenceManager();
    private SequenceHandler handler;

    private SequenceManager() {
    }

    public static void init() {
        int seqHandlerType = ClusterConfig.getInstance().getSequenceHandlerType();
        INSTANCE.handler = newSequenceHandler(seqHandlerType);
    }

    private static SequenceHandler newSequenceHandler(int seqHandlerType) {
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
                return new IncrSequenceMySQLHandler();
            case ClusterConfig.SEQUENCE_HANDLER_LOCAL_TIME:
                return new IncrSequenceTimeHandler();
            case ClusterConfig.SEQUENCE_HANDLER_ZK_DISTRIBUTED:
                if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
                    return new DistributedSequenceHandler();
                } else {
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk clusetr");
                }
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
                    return new IncrSequenceZKHandler();
                } else {
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk clusetr");
                }
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequnce handler type " + seqHandlerType);
        }
    }

    public static void load(RawJson sequenceJson, Set<String> currentShardingNodes) {
        if (INSTANCE.handler == null)
            return;
        INSTANCE.handler.load(sequenceJson, currentShardingNodes);
    }

    public static void reload(RawJson sequenceJson, Set<String> currentShardingNodes) {
        if (INSTANCE.handler == null)
            return;
        int seqHandlerType = ClusterConfig.getInstance().getSequenceHandlerType();
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                ReloadLogHelper.briefInfo("loadSequence ...");
                INSTANCE.handler.load(sequenceJson, currentShardingNodes);
                break;
            default:
                break;
        }
    }

    public static void tryLoad(RawJson sequenceJson, Set<String> currentShardingNodes, Logger logger) {
        int seqHandlerType = ClusterConfig.getInstance().getSequenceHandlerType();
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                logger.info("[dry-run] loadSequence ...");
                SequenceHandler tmpHandler = newSequenceHandler(seqHandlerType);
                tmpHandler.tryLoad(sequenceJson, currentShardingNodes);
                break;
            default:
                break;
        }
    }

    public static long nextId(String prefixName, @Nullable FrontendService frontendService) throws SQLNonTransientException {
        if (INSTANCE.handler == null)
            throw new ConfigException("sequence is not init");
        return INSTANCE.handler.nextId(prefixName, frontendService);
    }

    public static SequenceHandler getHandler() {
        return INSTANCE.handler;
    }

    public static Set<String> getShardingNodes(RawJson sequenceJson) {
        if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL && sequenceJson != null) {
            return IncrSequenceMySQLHandler.getShardingNodes(sequenceJson);
        }
        return Sets.newHashSet();
    }
}
