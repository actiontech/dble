/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.route.sequence.handler.*;

/**
 * Created by szf on 2019/9/19.
 */
public final class SequenceManager {
    private static final SequenceManager INSTANCE = new SequenceManager();
    private volatile SequenceHandler handler;

    private SequenceManager() {

    }

    public static void init(int seqHandlerType) {
        switch (seqHandlerType) {
            case ClusterConfig.SEQUENCE_HANDLER_MYSQL:
                INSTANCE.handler = new IncrSequenceMySQLHandler();
                break;
            case ClusterConfig.SEQUENCE_HANDLER_LOCAL_TIME:
                INSTANCE.handler = new IncrSequenceTimeHandler();
                break;
            case ClusterConfig.SEQUENCE_HANDLER_ZK_DISTRIBUTED:
                if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
                    INSTANCE.handler = new DistributedSequenceHandler();
                } else {
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk cluster");
                }
                break;
            case ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
                    INSTANCE.handler = new IncrSequenceZKHandler();
                } else {
                    throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType + " for no-zk cluster");
                }
                break;
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequence handler type " + seqHandlerType);
        }
    }

    public static void load(boolean lowerCaseTableNames) {
        INSTANCE.handler.load(lowerCaseTableNames);
    }

    public static void load(boolean lowerCaseTableNames, RawJson sequenceJson) {
        INSTANCE.handler.loadByJson(lowerCaseTableNames, sequenceJson);
    }

    public static SequenceManager getInstance() {
        return INSTANCE;
    }

    public static SequenceHandler getHandler() {
        return INSTANCE.handler;
    }
}
