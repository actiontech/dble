package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

import static com.actiontech.dble.DbleServer.TIMER_WORKER_NAME;

public class ThreadChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger("ThreadChecker");
    private static long checkTimeNs = 10 * 1000 * 1000000L; // 10s

    private static final ThreadChecker INSTANCE = new ThreadChecker();
    public final Map<Thread, TimeRecord> timeRecords = new ConcurrentHashMap<>(4);

    public void startExec(Thread t) {
        long ns = System.nanoTime();
        TimeRecord tr = timeRecords.get(t);
        if (tr == null) {
            tr = new TimeRecord();
            timeRecords.put(t, tr);
        }
        tr.setStartTime(ns);
        tr.setEndTime(0);
    }

    public void endExec() {
        TimeRecord tr = timeRecords.get(Thread.currentThread());
        long ns = System.nanoTime();
        if (tr != null) {
            tr.setEndTime(ns);
        }
    }

    public void terminated() {
        timeRecords.remove(Thread.currentThread());
    }

    public void doSelfCheck() {
        // check single thread
        for (Map.Entry<Thread, TimeRecord> entry : timeRecords.entrySet()) {
            doSelfCheck0(entry.getKey(), entry.getValue());
        }
        tryResolveThreadPoolAlarm();
    }

    private void doSelfCheck0(Thread th, TimeRecord tr) {
        String threadName = th.getName();
        LastRecordInfo current = getInfo(threadName, th, tr.getStartTime(), tr.getEndTime());
        if (current == null) return;
        if (tr.getLastInfo() == null) {
            tr.setLastInfo(current);
        } else {
            LastRecordInfo previous = tr.getLastInfo();
            tr.setLastInfo(current);
            boolean isSuspected = false;
            String key = current.getName();
            // diff
            if (current.getEndTime() == 0 && previous.getEndTime() == 0 && current.getStartTime() == previous.getStartTime()) {
                long nowTime = System.nanoTime();
                long timeDiff = nowTime - current.getStartTime();
                if (timeDiff > checkTimeNs) { // more than 10s will log
                    String msg = "Thread[" + key + "] suspected hang, execute time:[{" + timeDiff / 1000000L + "ms}] more than 10s, currentState:[" + current.getState() + "]";
                    LOGGER.info(msg + ", stackTrace: {}", getStackTrace(key));
                    // if there is task accumulation in the queue, it means that all threads are hang
                    if (previous.getCompletedTask() == current.getCompletedTask() && current.getActiveTaskCount() == previous.getActiveTaskCount()) {
                        LOGGER.info("The thread pool where the thread[" + key + "] is located is in the hang state and cannot work. Trigger alarm");
                        isSuspected = true;
                        Map<String, String> labels = AlertUtil.genSingleLabel("thread", key);
                        AlertUtil.alertSelf(AlarmCode.THREAD_SUSPECTED_HANG, Alert.AlertLevel.WARN, msg, labels);
                        ToResolveContainer.THREAD_SUSPECTED_HANG.add(key);
                        return;
                    }
                }
            }
            if (!isSuspected && ToResolveContainer.THREAD_SUSPECTED_HANG.contains(key)) {
                LOGGER.info("Resolve Thread[" + key + "]'s alarm");
                AlertUtil.alertSelfResolve(AlarmCode.THREAD_SUSPECTED_HANG, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("thread", key),
                        ToResolveContainer.THREAD_SUSPECTED_HANG, key);
                return;
            }
        }
    }

    private void tryResolveThreadPoolAlarm() {
        NameableExecutor timerExecutor = (NameableExecutor) DbleServer.getInstance().getTimerExecutor();
        String key = timerExecutor.getName();
        if (!ToResolveContainer.THREAD_SUSPECTED_HANG.contains(key))
            return;
        if (!timerExecutor.isShutdown() && !timerExecutor.isTerminated() &&
                timerExecutor.getQueue().size() != 1024 && timerExecutor.getPoolSize() != timerExecutor.getActiveCount()) {
            LOGGER.info("Resolve ThreadPool[" + key + "]'s alarm");
            AlertUtil.alertSelfResolve(AlarmCode.THREAD_SUSPECTED_HANG, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("thread", key),
                    ToResolveContainer.THREAD_SUSPECTED_HANG, key);
            return;
        }
    }

    public void timerExecuteError(RejectedExecutionException exception, String method) {
        NameableExecutor timerExecutor = (NameableExecutor) DbleServer.getInstance().getTimerExecutor();
        String key = timerExecutor.getName();
        if (!ToResolveContainer.THREAD_SUSPECTED_HANG.contains(key)) { // only one
            String msg = "ThreadPool[" + TIMER_WORKER_NAME + "] execute fail, isShutDown[" + timerExecutor.
                    isShutdown() + "], task_queue_size[" + timerExecutor.getQueue().size() + "]";

            LOGGER.info(msg + ", happened at '{}' method, exception:{}", method, exception);
            LOGGER.info("Trigger ThreadPool[" + key + "]'s alarm");

            Map<String, String> labels = AlertUtil.genSingleLabel("thread", key);
            AlertUtil.alertSelf(AlarmCode.THREAD_SUSPECTED_HANG, Alert.AlertLevel.WARN, msg + ", For details, see logs/thread.log", labels);
            ToResolveContainer.THREAD_SUSPECTED_HANG.add(key);
        }
    }

    private String getStackTrace(String threadName) {
        for (Map.Entry<Thread, TimeRecord> entry : timeRecords.entrySet()) {
            StackTraceElement[] st = entry.getKey().getStackTrace();
            if (threadName.equals(entry.getKey().getName())) {
                StringBuilder sbf = new StringBuilder();
                for (StackTraceElement e : st) {
                    sbf.append("\n\tat ");
                    sbf.append(e);
                }
                return sbf.toString();
            }
        }
        return "empty";
    }

    private LastRecordInfo getInfo(String name, Thread th, long lastExecTime, long lastFinishTime) {
        String[] arr = name.split("-");
        if (arr.length == 2) {
            switch (arr[1]) {
                case TIMER_WORKER_NAME:
                    NameableExecutor exec = (NameableExecutor) DbleServer.getInstance().getTimerExecutor();
                    return new LastRecordInfo(name, th.getState(), lastExecTime, lastFinishTime, exec.getActiveCount(), exec.getQueue().size(), exec.getCompletedTaskCount());
                default:
                    return null;
            }
        }
        return null;
    }

    public static ThreadChecker getInstance() {
        return INSTANCE;
    }

    static class TimeRecord {
        volatile long startTime;
        volatile long endTime;
        volatile LastRecordInfo lastInfo;

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public LastRecordInfo getLastInfo() {
            return lastInfo;
        }

        public void setLastInfo(LastRecordInfo lastInfo) {
            this.lastInfo = lastInfo;
        }
    }

    static class LastRecordInfo {
        String name;
        long startTime;
        long endTime;
        long activeTaskCount;
        long taskQueueSize;
        long completedTask;
        Thread.State state;

        LastRecordInfo(String name, Thread.State state, long startTime, long endTime, long activeTaskCount, long taskQueueSize, long completedTask) {
            this.name = name;
            this.state = state;
            this.startTime = startTime;
            this.endTime = endTime;
            this.activeTaskCount = activeTaskCount;
            this.taskQueueSize = taskQueueSize;
            this.completedTask = completedTask;
        }

        public Thread.State getState() {
            return state;
        }

        public String getName() {
            return name;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public long getActiveTaskCount() {
            return activeTaskCount;
        }

        public long getTaskQueueSize() {
            return taskQueueSize;
        }

        public long getCompletedTask() {
            return completedTask;
        }
    }
}
