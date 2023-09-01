/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.statistic.stat.FrontActiveRatioStat;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Iterator;
import java.util.concurrent.*;

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
        this.scheduledExecutor = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setNameFormat("TimerScheduler-%d").build());
    }

    public void init(ExecutorService executor) {
        this.timerExecutor = executor;
        scheduledExecutor.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(DbleServer.getInstance().processorCheck(), 0L, SystemConfig.getInstance().getProcessorCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(dbInstanceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(oldDbGroupClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(oldDbInstanceClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(xaSessionCheck(), 0L, SystemConfig.getInstance().getXaSessionCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleWithFixedDelay(xaLogClean(), 0L, SystemConfig.getInstance().getXaLogCleanPeriod(), TimeUnit.MILLISECONDS);
        scheduledExecutor.scheduleAtFixedRate(threadStatRenew(), 0L, 1, TimeUnit.SECONDS);
        if (FrontActiveRatioStat.getInstance().isEnable()) {
            scheduledExecutor.scheduleWithFixedDelay(compressionsActiveStat(), 0L, FrontActiveRatioStat.INTERVAL, TimeUnit.MILLISECONDS);
        }
        scheduledExecutor.scheduleAtFixedRate(printLongTimeDDL(), 0L, DDL_EXECUTE_CHECK_PERIOD, TimeUnit.SECONDS);
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
                try {
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
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "dbInstanceOldConsClear()");
                }
            }
        };
    }

    /**
     * after reload @@config_all ,clean old dbGroup
     */
    private Runnable oldDbGroupClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    timerExecutor.execute(() -> {
                        Iterator<PhysicalDbGroup> iterator = IOProcessor.BACKENDS_OLD_GROUP.iterator();
                        while (iterator.hasNext()) {
                            PhysicalDbGroup dbGroup = iterator.next();
                            boolean isStop = dbGroup.stopOfBackground("[background task]reload config, recycle old group");
                            LOGGER.info("[background task]recycle old group:{},result:{}", dbGroup.getGroupName(), isStop);
                            if (isStop) {
                                iterator.remove();
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "oldDbGroupClear()");
                }
            }
        };
    }

    /**
     * after reload @@config_all ,clean old dbInstance
     */
    private Runnable oldDbInstanceClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    timerExecutor.execute(() -> {
                        Iterator<PhysicalDbInstance> iterator = IOProcessor.BACKENDS_OLD_INSTANCE.iterator();
                        while (iterator.hasNext()) {
                            PhysicalDbInstance dbInstance = iterator.next();
                            boolean isStop = dbInstance.stopOfBackground("[background task]reload config, recycle old dbInstance");
                            LOGGER.info("[background task]recycle old dbInstance:{},result:{}", dbInstance, isStop);
                            if (isStop) {
                                iterator.remove();
                                dbInstance.getDbGroup().setState(PhysicalDbGroup.INITIAL);
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "oldDbInstanceClear()");
                }
            }
        };
    }


    // XA session check job
    private Runnable xaSessionCheck() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    timerExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            XASessionCheck.getInstance().checkSessions();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "xaSessionCheck()");
                }
            }
        };
    }

    private Runnable xaLogClean() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    timerExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            XAStateLog.cleanCompleteRecoveryLog();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "xaLogClean()");
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

    public Runnable compressionsActiveStat() {
        return new Runnable() {
            @Override
            public void run() {
                FrontActiveRatioStat.getInstance().compress();
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
}
