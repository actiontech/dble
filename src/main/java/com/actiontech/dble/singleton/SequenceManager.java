package com.actiontech.dble.singleton;

import com.actiontech.dble.config.model.SystemConfig;
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
            case SystemConfig.SEQUENCE_HANDLER_MYSQL:
                INSTANCE.handler = new IncrSequenceMySQLHandler();
                break;
            case SystemConfig.SEQUENCE_HANDLER_LOCAL_TIME:
                INSTANCE.handler = new IncrSequenceTimeHandler();
                break;
            case SystemConfig.SEQUENCE_HANDLER_ZK_DISTRIBUTED:
                INSTANCE.handler = new DistributedSequenceHandler();
                break;
            case SystemConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                INSTANCE.handler = new IncrSequenceZKHandler();
                break;
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequnce handler type " + seqHandlerType);
        }
    }

    public static void load(boolean lowerCaseTableNames) {
        INSTANCE.handler.load(lowerCaseTableNames);
    }

    public static SequenceManager getInstance() {
        return INSTANCE;
    }

    public static SequenceHandler getHandler() {
        return INSTANCE.handler;
    }
}
