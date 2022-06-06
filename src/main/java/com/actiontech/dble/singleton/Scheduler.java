/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAnalysisHandler;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.statistic.stat.*;
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
    private static final int DEFAULT_CHECK_XAID = 5;
    private ExecutorService timerExecutor;
    private ScheduledExecutorService scheduledExecutor;

    private Scheduler() {
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TimerScheduler-%d").build());
    }

    public void init(ExecutorService executor) {
        this.timerExecutor = executor;
        scheduledExecutor.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(DbleServer.getInstance().processorCheck(), 0L, SystemConfig.getInstance().getProcessorCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(dbInstanceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(oldDbGroupClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(xaSessionCheck(), 0L, SystemConfig.getInstance().getXaSessionCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(xaLogClean(), 0L, SystemConfig.getInstance().getXaLogCleanPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(resultSetMapClear(), 0L, SystemConfig.getInstance().getClearBigSQLResultSetMapMs(), TimeUnit.MILLISECONDS);
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            //sql record detail timing clean
            scheduledExecutor.scheduleWithFixedDelay(recycleSqlStat(), 0L, DEFAULT_SQL_STAT_RECYCLE_PERIOD, TimeUnit.MILLISECONDS);
        }
        scheduledExecutor.scheduleAtFixedRate(threadStatRenew(), 0L, 1, TimeUnit.SECONDS);
        if (FrontActiveRatioStat.getInstance().isEnable()) {
            scheduledExecutor.scheduleWithFixedDelay(clearStaleData(), 0L, FrontActiveRatioStat.LAST_STAT_MAX, TimeUnit.MILLISECONDS);
        }
        scheduledExecutor.scheduleAtFixedRate(printLongTimeDDL(), 0L, DDL_EXECUTE_CHECK_PERIOD, TimeUnit.SECONDS);
        scheduledExecutor.scheduleWithFixedDelay(checkResidualXid(), 0L, DEFAULT_CHECK_XAID, TimeUnit.MINUTES);
    }

    private Runnable printLongTimeDDL() {
        return new Runnable() {
            @Override
            public void run() {
                DDLTraceHelper.printDDLOutOfLimit();
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

    /**
     * after reload @@config_all ,clean old connection
     */
    private Runnable dbInstanceOldConsClear() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        long sqlTimeout = SystemConfig.getInstance().getSqlExecuteTimeout() * 1000L;
                        //close connection if now -lastTime>sqlExecuteTimeout
                        long currentTime = TimeUtil.currentTimeMillis();
                        Iterator<PooledConnection> iterator = IOProcessor.BACKENDS_OLD.iterator();
                        while (iterator.hasNext()) {
                            PooledConnection con = iterator.next();
                            long lastTime = con.getLastTime();
                            if (con.isClosed() || con.getState() != PooledConnection.STATE_IN_USE || currentTime - lastTime > sqlTimeout) {
                                con.close("clear old backend connection ...");
                                iterator.remove();
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
    private Runnable oldDbGroupClear() {
        return () -> timerExecutor.execute(() -> {

            Iterator<PhysicalDbGroup> iterator = IOProcessor.BACKENDS_OLD_GROUP.iterator();
            while (iterator.hasNext()) {
                PhysicalDbGroup dbGroup = iterator.next();
                boolean isStop = dbGroup.stopOfBackground("[background task]reload config, recycle old group");
                LOGGER.info("[background task]recycle old group`{}` result{}", dbGroup.getGroupName(), isStop);
                if (isStop) {
                    iterator.remove();
                }
            }
        });
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
                    if (bufferUsagePercent < SystemConfig.getInstance().getBufferUsagePercent()) {
                        Map<UserName, UserStat> map = UserStatAnalyzer.getInstance().getUserStatMap();
                        Set<UserName> userSet = DbleServer.getInstance().getConfig().getUsers().keySet();
                        for (UserName user : userSet) {
                            UserStat userStat = map.get(user);
                            if (userStat != null) {
                                SqlResultSizeRecorder recorder = userStat.getSqlResultSizeRecorder();
                                recorder.clearSqlResultSet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.info("resultSetMapClear err ", e);
                }
            }

        };
    }


    //clean up the old data in SqlStat
    private Runnable recycleSqlStat() {
        return new Runnable() {
            @Override
            public void run() {
                Map<UserName, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
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

    public Runnable clearStaleData() {
        return new Runnable() {
            @Override
            public void run() {
                FrontActiveRatioStat.getInstance().clearStaleData();
            }
        };
    }

    public ExecutorService getTimerExecutor() {
        return timerExecutor;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public static Scheduler getInstance() {
        return INSTANCE;
    }

    public Runnable checkResidualXid() {
        return new Runnable() {
            @Override
            public void run() {
                new XAAnalysisHandler().
                        checkResidualTask();
            }
        };
    }
}
