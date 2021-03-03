package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class StatisticListener {
    private static final StatisticListener INSTANCE = new StatisticListener();
    private volatile boolean enable = false;

    private volatile ConcurrentHashMap<Session, StatisticRecord> recorders = new ConcurrentHashMap<>(16);
    private final LongAdder virtualTxID = new LongAdder();

    public void start() {
        if (enable) return;
        enable = true;
        for (IOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (!front.isManager()) {
                    if (front.getService() instanceof ShardingService) {
                        NonBlockingSession session = ((ShardingService) front.getService()).getSession2();
                        if (!session.closed()) {
                            register(session);
                        }
                    } else if (front.getService() instanceof RWSplitService) {
                        RWSplitNonBlockingSession session = ((RWSplitService) front.getService()).getSession();
                        if (!session.closed()) {
                            register(session);
                        }
                    }
                }
            }
        }
    }

    public void stop() {
        enable = false;
        recorders.clear();
    }

    public long getIncrementVirtualTxID() {
        virtualTxID.increment();
        return virtualTxID.longValue();
    }

    public void register(Session session) {
        if (enable) {
            if (!recorders.keySet().contains(session)) {
                if (session instanceof NonBlockingSession) {
                    recorders.put(session, new ShardingStatisticRecord(((NonBlockingSession) session).getShardingService()));
                } else if (session instanceof RWSplitNonBlockingSession) {
                    recorders.put(session, new RwSplitStatisticRecord(((RWSplitNonBlockingSession) session).getService()));
                }
            }
        }
    }

    public StatisticRecord getRecorder(Session session) {
        if (enable && session != null) {
            return recorders.get(session);
        }
        return null;
    }

    public StatisticRecord getRecorder(AbstractService service) {
        if (enable) {
            if (service instanceof ShardingService) {
                return recorders.get(((ShardingService) service).getSession2());
            } else if (service instanceof RWSplitService) {
                return recorders.get(((RWSplitService) service).getSession());
            }
        }
        return null;
    }

    public void remove(Session session) {
        if (enable) {
            recorders.remove(session);
        }
    }

    public void remove(AbstractService service) {
        if (service instanceof ShardingService) {
            remove(((ShardingService) service).getSession2());
        } else if (service instanceof RWSplitService) {
            remove(((RWSplitService) service).getSession());
        }
    }

    public static StatisticListener getInstance() {
        return INSTANCE;
    }
}
