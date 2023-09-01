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
import com.actiontech.dble.util.NameableScheduledThreadPoolExecutor;
import com.actiontech.dble.util.TimeUtil;

import java.util.Iterator;
import java.util.concurrent.*;
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

    public void init() {
        NameableScheduledThreadPoolExecutor scheduledExecutor = DbleServer.getInstance().getTimerSchedulerExecutor();
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
                try {
                    DDLTraceHelper.printDDLOutOfLimit();
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task printLongTimeDDL() happen exception:{} ", e.getMessage());
                }
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
                    DbleServer.getInstance().getTimerExecutor().execute(new Runnable() {
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
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task dbInstanceOldConsClear() happen exception:{} ", e.getMessage());
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
                    DbleServer.getInstance().getTimerExecutor().execute(() -> {
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
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task oldDbGroupClear() happen exception:{} ", e.getMessage());
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
                    DbleServer.getInstance().getTimerExecutor().execute(() -> {
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
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task oldDbInstanceClear() happen exception:{} ", e.getMessage());
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
                    DbleServer.getInstance().getTimerExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            XASessionCheck.getInstance().checkSessions();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "xaSessionCheck()");
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task xaSessionCheck() happen exception:{} ", e.getMessage());
                }
            }
        };
    }

    private Runnable xaLogClean() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    DbleServer.getInstance().getTimerExecutor().execute(new Runnable() {
                        @Override
                        public void run() {
                            XAStateLog.cleanCompleteRecoveryLog();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    ThreadChecker.getInstance().timerExecuteError(e, "xaLogClean()");
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task xaLogClean() happen exception:{} ", e.getMessage());
                }
            }
        };
    }

    public Runnable threadStatRenew() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    for (ThreadWorkUsage obj : DbleServer.getInstance().getThreadUsedMap().values()) {
                        obj.switchToNew();
                    }
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task threadStatRenew() happen exception:{} ", e.getMessage());
                }
            }
        };
    }

    public Runnable compressionsActiveStat() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    FrontActiveRatioStat.getInstance().compress();
                } catch (Throwable e) {
                    LOGGER.warn("scheduled task compressionsActiveStat() happen exception:{} ", e.getMessage());
                }
            }
        };
    }

    public static Scheduler getInstance() {
        return INSTANCE;
    }
}
