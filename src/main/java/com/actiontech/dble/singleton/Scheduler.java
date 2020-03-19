package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.statistic.stat.SqlResultSizeRecorder;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.actiontech.dble.server.NonBlockingSession.LOGGER;


/**
 * Created by szf on 2019/9/19.
 */
public final class Scheduler {
    private static final Scheduler INSTANCE = new Scheduler();
    private static final long TIME_UPDATE_PERIOD = 20L;
    private static final long DDL_EXECUTE_CHECK_PERIOD = 60L;
    private static final long DEFAULT_OLD_CONNECTION_CLEAR_PERIOD = 5 * 1000L;
    private static final long DEFAULT_SQL_STAT_RECYCLE_PERIOD = 5 * 1000L;
    private ExecutorService timerExecutor;

    public void init(SystemConfig system, ExecutorService executor) {
        this.timerExecutor = executor;
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TimerScheduler-%d").build());
        long dataNodeIdleCheckPeriod = system.getDataNodeIdleCheckPeriod();
        scheduler.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(DbleServer.getInstance().processorCheck(), 0L, system.getProcessorCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataNodeConHeartBeatCheck(dataNodeIdleCheckPeriod), 0L, dataNodeIdleCheckPeriod, TimeUnit.MILLISECONDS);
        //dataHost heartBeat  will be influence by dataHostWithoutWR
        scheduler.scheduleAtFixedRate(dataSourceHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataSourceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(xaSessionCheck(), 0L, system.getXaSessionCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(xaLogClean(), 0L, system.getXaLogCleanPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(resultSetMapClear(), 0L, system.getClearBigSqLResultSetMapMs(), TimeUnit.MILLISECONDS);
        if (system.getUseSqlStat() == 1) {
            //sql record detail timing clean
            scheduler.scheduleWithFixedDelay(recycleSqlStat(), 0L, DEFAULT_SQL_STAT_RECYCLE_PERIOD, TimeUnit.MILLISECONDS);
        }
        scheduler.scheduleAtFixedRate(threadStatRenew(), 0L, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(printLongTimeDDL(), 0L, DDL_EXECUTE_CHECK_PERIOD, TimeUnit.SECONDS);
    }

    private Runnable printLongTimeDDL() {
        return new Runnable() {
            @Override
            public void run() {
                DDLTraceManager.getInstance().printDDLOutOfLimit();
            }
        };
    }

    private Runnable updateTime() {
        return new Runnable() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    private Runnable dataNodeConHeartBeatCheck(final long heartPeriod) {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        Map<String, PhysicalDataHost> nodes = DbleServer.getInstance().getConfig().getDataHosts();
                        for (PhysicalDataHost node : nodes.values()) {
                            node.heartbeatCheck(heartPeriod);
                        }
                    }
                });
            }
        };
    }

    // heartbeat for data source
    private Runnable dataSourceHeartbeat() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
                            Map<String, PhysicalDataHost> hosts = DbleServer.getInstance().getConfig().getDataHosts();
                            for (PhysicalDataHost host : hosts.values()) {
                                host.doHeartbeat();
                            }
                        }
                    }
                });
            }
        };
    }


    /**
     * after reload @@config_all ,clean old connection
     */
    private Runnable dataSourceOldConsClear() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        long sqlTimeout = DbleServer.getInstance().getConfig().getSystem().getSqlExecuteTimeout() * 1000L;
                        //close connection if now -lastTime>sqlExecuteTimeout
                        long currentTime = TimeUtil.currentTimeMillis();
                        Iterator<BackendConnection> iterator = NIOProcessor.BACKENDS_OLD.iterator();
                        while (iterator.hasNext()) {
                            BackendConnection con = iterator.next();
                            long lastTime = con.getLastTime();
                            if (con.isClosed() || !con.isBorrowed() || currentTime - lastTime > sqlTimeout) {
                                con.close("clear old backend connection ...");
                                iterator.remove();
                            }
                        }
                    }
                });
            }

        };
    }


    // XA session check job
    private Runnable xaSessionCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        XASessionCheck.getInstance().checkSessions();
                    }
                });
            }
        };
    }

    private Runnable xaLogClean() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        XAStateLog.cleanCompleteRecoveryLog();
                    }
                });
            }
        };
    }


    /**
     * clean up the data in UserStatAnalyzer
     */
    private Runnable resultSetMapClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    BufferPool pool = BufferPoolManager.getBufferPool();
                    long bufferSize = pool.size();
                    long bufferCapacity = pool.capacity();
                    long bufferUsagePercent = (bufferCapacity - bufferSize) * 100 / bufferCapacity;
                    if (bufferUsagePercent < DbleServer.getInstance().getConfig().getSystem().getBufferUsagePercent()) {
                        Map<String, UserStat> map = UserStatAnalyzer.getInstance().getUserStatMap();
                        Set<String> userSet = DbleServer.getInstance().getConfig().getUsers().keySet();
                        for (String user : userSet) {
                            UserStat userStat = map.get(user);
                            if (userStat != null) {
                                SqlResultSizeRecorder recorder = userStat.getSqlResultSizeRecorder();
                                recorder.clearSqlResultSet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.info("resultSetMapClear err " + e);
                }
            }

        };
    }


    //clean up the old data in SqlStat
    private Runnable recycleSqlStat() {
        return new Runnable() {
            @Override
            public void run() {
                Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
                for (UserStat userStat : statMap.values()) {
                    userStat.getSqlLastStat().recycle();
                    userStat.getSqlRecorder().recycle();
                    userStat.getSqlHigh().recycle();
                    userStat.getSqlLargeRowStat().recycle();
                }
            }
        };
    }

    public Runnable threadStatRenew() {
        return new Runnable() {
            @Override
            public void run() {
                for (ThreadWorkUsage obj : DbleServer.getInstance().getThreadUsedMap().values()) {
                    obj.switchToNew();
                }
            }
        };
    }

    public ExecutorService getTimerExecutor() {
        return timerExecutor;
    }


    public static Scheduler getInstance() {
        return INSTANCE;
    }

}
