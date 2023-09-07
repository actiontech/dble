package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.mysql.xa.XaCheckHandler;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.executor.BackendCurrentRunnable;
import com.actiontech.dble.net.executor.FrontendBlockRunnable;
import com.actiontech.dble.net.executor.FrontendCurrentRunnable;
import com.actiontech.dble.net.executor.WriteToBackendRunnable;
import com.actiontech.dble.net.impl.nio.RW;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.util.ExecutorUtil;
import com.actiontech.dble.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;

import static com.actiontech.dble.DbleServer.*;

public final class ThreadManager {
    private static final Logger LOGGER = LoggerFactory.getLogger("ThreadChecker");

    private ThreadManager() {
    }

    // single thread
    public static void interruptSingleThread(String threadName) throws Exception {
        Thread[] threads = getAllThread();
        Thread find = null;
        for (Thread thread : threads) {
            if (thread.getName().equals(threadName)) {
                find = thread;
                break;
            }
        }
        if (find == null)
            throw new Exception("Thread[" + threadName + "] does not exist");
        find.interrupt();
        LOGGER.info("exec interrupt Thread[{}]", find.getName());
    }

    public static void recoverSingleThread(String threadName) throws Exception {
        String[] array = threadName.split("-");
        if (array.length == 2) {
            DbleServer server = DbleServer.getInstance();
            switch (array[1]) {
                case FRONT_WORKER_NAME:
                    NameableExecutor nameableExecutor0 = (NameableExecutor) server.getFrontExecutor();
                    if (nameableExecutor0.getPoolSize() > nameableExecutor0.getActiveCount()) {
                        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                            nameableExecutor0.execute(new FrontendCurrentRunnable(server.getFrontHandlerQueue()));
                        } else {
                            nameableExecutor0.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) server.getFrontHandlerQueue()));
                        }
                    } else {
                        throw new Exception("threadPool[{" + FRONT_WORKER_NAME + "}] does not need to be recover");
                    }
                    break;
                case FRONT_MANAGER_WORKER_NAME:
                    NameableExecutor nameableExecutor1 = (NameableExecutor) server.getFrontExecutor();
                    if (nameableExecutor1.getPoolSize() > nameableExecutor1.getActiveCount()) {
                        nameableExecutor1.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) server.getManagerFrontHandlerQueue()));
                    } else {
                        throw new Exception("threadPool[{" + FRONT_MANAGER_WORKER_NAME + "}] does not need to be recover");
                    }
                    break;
                case BACKEND_WORKER_NAME:
                    NameableExecutor nameableExecutor2 = (NameableExecutor) server.getBackendExecutor();
                    if (nameableExecutor2.getPoolSize() > nameableExecutor2.getActiveCount()) {
                        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
                            nameableExecutor2.execute(new BackendCurrentRunnable(server.getConcurrentBackHandlerQueue()));
                        }
                    } else {
                        throw new Exception("threadPool[{" + BACKEND_WORKER_NAME + "}] does not need to be recover");
                    }
                    break;
                case WRITE_TO_BACKEND_WORKER_NAME:
                    NameableExecutor nameableExecutor3 = (NameableExecutor) server.getWriteToBackendExecutor();
                    if (nameableExecutor3.getPoolSize() > nameableExecutor3.getActiveCount()) {
                        nameableExecutor3.execute(new WriteToBackendRunnable(server.getWriteToBackendQueue()));
                    } else {
                        throw new Exception("threadPool[{" + WRITE_TO_BACKEND_WORKER_NAME + "}] does not need to be recover");
                    }
                    break;
                case NIO_FRONT_RW:
                    if (SystemConfig.getInstance().getUsingAIO() != 1) {
                        try {
                            NameableExecutor nameableExecutor4 = (NameableExecutor) server.getNioFrontExecutor();
                            if (nameableExecutor4.getPoolSize() > nameableExecutor4.getActiveCount()) {
                                nameableExecutor4.execute(new RW(server.getFrontRegisterQueue()));
                            } else {
                                throw new Exception("threadPool[{" + NIO_FRONT_RW + "}] does not need to be recover");
                            }
                        } catch (IOException e) {
                            throw new Exception("recover threadPool[{" + NIO_FRONT_RW + "}] fail", e);
                        }
                    }
                    break;
                case NIO_BACKEND_RW:
                    if (SystemConfig.getInstance().getUsingAIO() != 1) {
                        try {
                            NameableExecutor nameableExecutor5 = (NameableExecutor) server.getNioBackendExecutor();
                            if (nameableExecutor5.getPoolSize() > nameableExecutor5.getActiveCount()) {
                                nameableExecutor5.execute(new RW(server.getBackendRegisterQueue()));
                            } else {
                                throw new Exception("threadPool[{" + NIO_BACKEND_RW + "}] does not need to be recover");
                            }
                        } catch (IOException e) {
                            throw new Exception("recover threadPool[{" + NIO_BACKEND_RW + "}] fail", e);
                        }
                    }
                    break;
                default:
                    throw new Exception("The recover operation of threadPool[" + threadName + "] is not supported");
            }
        } else {
            throw new Exception("The recover operation of threadPool[" + threadName + "] is not supported");
        }
    }

    // thread pool（TIMER_WORKER_NAME、TIMER_SCHEDULER_WORKER_NAME）
    public static void shutDownThreadPool(String threadPoolName) throws Exception {
        switch (threadPoolName) {
            case TIMER_WORKER_NAME:
                if (DbleServer.getInstance().getTimerExecutor().isShutdown()) {
                    throw new Exception("threadPool[" + TIMER_WORKER_NAME + "] already shutdown");
                }
                LOGGER.info("manual shutdown threadPool[{}] ... start ...", TIMER_WORKER_NAME);
                DbleServer.getInstance().getTimerExecutor().shutdownNow();
                LOGGER.info("manual shutdown threadPool[{}] ... end ...", TIMER_WORKER_NAME);
                break;
            case TIMER_SCHEDULER_WORKER_NAME:
                if (DbleServer.getInstance().getTimerSchedulerExecutor().isShutdown()) {
                    throw new Exception("threadPool[" + TIMER_SCHEDULER_WORKER_NAME + "] already shutdown");
                }
                /*
                 0、shutdown
                 1、stopHeartbeat
                 2、stopDelayDetection
                 3、stopXaIdCheckPeriod
                */
                LOGGER.info("manual shutdown threadPool[{}] ... start ...", TIMER_SCHEDULER_WORKER_NAME);
                DbleServer.getInstance().getTimerSchedulerExecutor().shutdownNow();
                Iterator<PhysicalDbGroup> iterator = DbleServer.getInstance().getConfig().getDbGroups().values().iterator();
                while (iterator.hasNext()) {
                    PhysicalDbGroup dbGroup = iterator.next();
                    LOGGER.info("dbGroup[{}] stopHeartbeat...", dbGroup.getGroupName());
                    dbGroup.stopHeartbeat("manual shutdown thread pool TimerScheduler");
                    LOGGER.info("dbGroup[{}] stopDelayDetection...", dbGroup.getGroupName());
                    dbGroup.stopDelayDetection("manual shutdown thread pool TimerScheduler");
                }
                LOGGER.info("stopXaIdCheckPeriod...");
                XaCheckHandler.stopXaIdCheckPeriod();
                LOGGER.info("manual shutdown threadPool[{}] ... end ...", TIMER_SCHEDULER_WORKER_NAME);
                break;
            default:
                throw new Exception("The shutdown operation of thread[" + TIMER_SCHEDULER_WORKER_NAME + "] is not supported");
        }
    }

    public static void recoverThreadPool(String threadName) throws Exception {
        switch (threadName) {
            case TIMER_WORKER_NAME:
                if (!DbleServer.getInstance().getTimerExecutor().isShutdown()) {
                    throw new Exception("threadPool[" + TIMER_WORKER_NAME + "] is not shutdown, no need to recover");
                }
                LOGGER.info("manual recover threadPool[{}] ... start ...", TIMER_WORKER_NAME);
                DbleServer.getInstance().setTimerExecutor(
                        ExecutorUtil.createTimer(TIMER_WORKER_NAME, 1, 2, ThreadChecker.getInstance()));
                LOGGER.info("manual recover threadPool[{}] ... end ...", TIMER_WORKER_NAME);
                break;
            case TIMER_SCHEDULER_WORKER_NAME:
                if (!DbleServer.getInstance().getTimerSchedulerExecutor().isShutdown()) {
                    throw new Exception("threadPool[" + TIMER_SCHEDULER_WORKER_NAME + "] is not shutdown, no need to recover");
                }
                /*
                 0、new TimerSchedulerExecutor AND init
                 1、startHeartbeat
                 2、startDelayDetection
                 3、startXaIdCheckPeriod
                 */
                LOGGER.info("manual recover threadPool[{}] ... start ...", TIMER_SCHEDULER_WORKER_NAME);
                DbleServer.getInstance().setTimerSchedulerExecutor(
                        ExecutorUtil.createFixedScheduled(TIMER_SCHEDULER_WORKER_NAME, 2, ThreadChecker.getInstance()));
                Scheduler.getInstance().init();
                Iterator<PhysicalDbGroup> iterator = DbleServer.getInstance().getConfig().getDbGroups().values().iterator();
                while (iterator.hasNext()) {
                    PhysicalDbGroup dbGroup = iterator.next();
                    LOGGER.info("dbGroup[{}] startHeartbeat...", dbGroup.getGroupName());
                    dbGroup.startHeartbeat();
                    LOGGER.info("dbGroup[{}] startDelayDetection...", dbGroup.getGroupName());
                    dbGroup.startDelayDetection();
                }
                LOGGER.info("startXaIdCheckPeriod...");
                XaCheckHandler.startXaIdCheckPeriod();
                LOGGER.info("manual recover threadPool[{}] ... end ...", TIMER_SCHEDULER_WORKER_NAME);
                break;
            default:
                throw new Exception("The recover operation of threadPool[" + threadName + "] is not supported");
        }
    }

    public static void printAll() {
        Thread[] threads = getAllThread();
        StringBuilder sbf = new StringBuilder();
        sbf.append("============== select all thread ============== start");
        for (Thread thread : threads) {
            sbf.append("\n \"");
            sbf.append(thread.getName());
            sbf.append("\" ");
            sbf.append("#");
            sbf.append(thread.getId());
            sbf.append(", state: ");
            sbf.append(thread.getState());
            sbf.append(", stackTrace: ");
            StackTraceElement[] st = thread.getStackTrace();
            for (StackTraceElement e : st) {
                sbf.append("\n\tat ");
                sbf.append(e);
            }
        }
        sbf.append("\n============== select all thread ============== end");
        LOGGER.info(sbf.toString());
    }

    public static void printSingleThread(String threadName) throws Exception {
        if (threadName != null && threadName.length() > 0) {
            Thread[] threads = getAllThread();
            StringBuilder sbf = null;
            for (Thread thread : threads) {
                if (thread.getName().equals(threadName)) {
                    sbf = new StringBuilder();
                    sbf.append("============== select thread[{" + threadName + "}] ============== start");
                    sbf.append("\n \"");
                    sbf.append(thread.getName());
                    sbf.append("\" ");
                    sbf.append("#");
                    sbf.append(thread.getId());
                    sbf.append(", state: ");
                    sbf.append(thread.getState());
                    sbf.append(", stackTrace: ");
                    StackTraceElement[] st = thread.getStackTrace();
                    for (StackTraceElement e : st) {
                        sbf.append("\n\tat ");
                        sbf.append(e);
                    }
                    sbf.append("\n============== select thread[{" + threadName + "}] ============== end");
                    break;
                }
            }
            if (sbf == null) {
                throw new Exception("Thread[" + threadName + "] does not exist");
            } else {
                LOGGER.info(sbf.toString());
            }
        }
    }

    private static Thread[] getAllThread() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }
        int slackSize = topGroup.activeCount() * 2;
        Thread[] slackThreads = new Thread[slackSize];
        int actualSize = topGroup.enumerate(slackThreads);
        Thread[] atualThreads = new Thread[actualSize];
        System.arraycopy(slackThreads, 0, atualThreads, 0, actualSize);
        return atualThreads;
    }
}
