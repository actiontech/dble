/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.route.sequence.handler.*;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Created by szf on 2019/9/19.
 */
public final class SequenceManager {
    private static final SequenceManager INSTANCE = new SequenceManager();
    private volatile SequenceHandler handler;

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
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk cluster");
                }
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
                    return new IncrSequenceZKHandler();
                } else {
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk cluster");
                }
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType);
        }
    }

    public static void load(RawJson sequenceJson) {
        if (INSTANCE.handler == null)
            return;
        INSTANCE.handler.load(sequenceJson, DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
    }

    public static void reload(RawJson sequenceJson) {
        if (INSTANCE.handler == null)
            return;
        int seqHandlerType = ClusterConfig.getInstance().getSequenceHandlerType();
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                INSTANCE.handler.load(sequenceJson, DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
                break;
            default:
                break;
        }
    }

    public static void tryLoad(RawJson sequenceJson) {
        int seqHandlerType = ClusterConfig.getInstance().getSequenceHandlerType();
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                SequenceHandler tmpHandler = newSequenceHandler(seqHandlerType);
                tmpHandler.tryLoad(sequenceJson, DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
                break;
            default:
                break;
        }
    }

    public static SequenceManager getInstance() {
        return INSTANCE;
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
