package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.DistinctHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OrderByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.TempTableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.AggregateHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinInnerHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.SubQueryHandler;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class FlowController {
    private static final FlowController INSTANCE = new FlowController();
    private volatile FlowControllerConfig config = null;

    private FlowController() {
    }

    public static void init() throws Exception {
        INSTANCE.config = new FlowControllerConfig(
                SystemConfig.getInstance().isEnableFlowControl(),
                SystemConfig.getInstance().getFlowControlStartThreshold(),
                SystemConfig.getInstance().getFlowControlStopThreshold());

        if (INSTANCE.config.getEnd() < 0 || INSTANCE.config.getStart() <= 0) {
            throw new Exception("The flowControlStartThreshold & flowControlStopThreshold must be positive integer");
        } else if (INSTANCE.config.getEnd() >= INSTANCE.config.getStart()) {
            throw new Exception("The flowControlStartThreshold must bigger than flowControlStopThreshold");
        }
    }

    // load data
    public static void tryFlowControl(final MySQLResponseService backendService) {
        if (INSTANCE.config.isEnableFlowControl() &&
                !backendService.getConnection().isFlowControlled() &&
                backendService.getConnection().getWriteQueue().size() > INSTANCE.config.getStart()) {
            backendService.getConnection().startFlowControl();
        }
        while (backendService.getConnection().isFlowControlled()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    // simpleQuery
    public static void tryFlowControl(final NonBlockingSession session) {
        int size = session.getSource().getWriteQueue().size();
        if (INSTANCE.config.isEnableFlowControl() &&
                !session.getShardingService().getConnection().isFlowControlled() &&
                size > INSTANCE.config.getStart()) {
            session.getSource().startFlowControl();
        }
    }

    // complexQuery
    public static void tryFlowControl2(boolean isSupportFlowControl, final NonBlockingSession session) {
        if (isSupportFlowControl)
            tryFlowControl(session);
    }

    // control on_the nio side, try to remove flow control
    public static int tryRemoveFlowControl(int flowControlCount, final AbstractConnection con) {
        if (con.isFlowControlled()) {
            if (!INSTANCE.config.isEnableFlowControl()) {
                con.stopFlowControl();
                return -1;
            } else if (flowControlCount == -1 ||
                    ((flowControlCount != -1) && (flowControlCount <= INSTANCE.config.getEnd()))) {
                int currentWriteQueueSize = con.getWriteQueue().size();
                if (currentWriteQueueSize <= INSTANCE.config.getEnd()) {
                    con.stopFlowControl();
                    return -1;
                } else {
                    return currentWriteQueueSize;
                }
            } else {
                return --flowControlCount;
            }
        } else {
            return -1;
        }
    }

    // when complexQuery receives rowEofResponse, try to remove flow control
    public static void tryRemoveFlowControl(final AbstractService service) {
        MySQLResponseService service1;
        if (service instanceof MySQLResponseService &&
                (service1 = ((MySQLResponseService) service)).
                        getSession().
                        getShardingService().isFlowControlled()) {
            service1.getSession().
                    releaseFlowCntroll(
                            service1.getConnection());
        }
    }

    public static FlowControllerConfig getFlowCotrollerConfig() {
        return INSTANCE.config;
    }

    public static void configChange(FlowControllerConfig newConfig) {
        INSTANCE.config = newConfig;
    }

    public static boolean isEnableFlowControl() {
        return INSTANCE.config.isEnableFlowControl();
    }

    public static int getFlowStart() {
        return INSTANCE.config.getStart();
    }

    public static int getFlowEnd() {
        return INSTANCE.config.getEnd();
    }

    public static boolean isSupportFlowControl(BaseDMLHandler handler) {
        BaseDMLHandler nextHandler = handler;
        while (true) {
            if (nextHandler == null) {
                return true;
            } else if (nextHandler instanceof SubQueryHandler ||
                    nextHandler instanceof AggregateHandler ||
                    nextHandler instanceof DistinctHandler ||
                    nextHandler instanceof OrderByHandler ||
                    nextHandler instanceof NotInHandler ||
                    nextHandler instanceof JoinHandler ||
                    nextHandler instanceof JoinInnerHandler ||
                    nextHandler instanceof DirectGroupByHandler ||
                    nextHandler instanceof TempTableHandler) { // These handler have a caching mechanism and do not adapt to flow control
                return false;
            } else {
                nextHandler = nextHandler.getNextHandler();
            }
        }
    }
}


