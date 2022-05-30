package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FrontActiveRatioStat {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontActiveRatioStat.class);
    private static final FrontActiveRatioStat INSTANCE = new FrontActiveRatioStat();
    private Map<FrontendConnection, WorkStat> usageStats;

    private boolean enable = false;

    public FrontActiveRatioStat() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1)
            return;
        enable = SystemConfig.getInstance().getEnableSessionActiveRatioStat() == 1;
        if (enable) {
            usageStats = new ConcurrentHashMap<>();
        }
    }

    public boolean isEnable() {
        return enable;
    }

    public static FrontActiveRatioStat getInstance() {
        return INSTANCE;
    }

    public void register(AbstractConnection connection, long time) {
        if (usageStats == null || !(connection instanceof FrontendConnection))
            return;
        usageStats.put((FrontendConnection) connection, newWorkStat(time));
    }

    public void record(AbstractConnection connection, Consumer<WorkStat> consumer) {
        if (usageStats == null || !(connection instanceof FrontendConnection))
            return;
        try {
            Optional.ofNullable(usageStats.get(connection)).ifPresent(consumer);
        } catch (Exception ex) {
            LOGGER.error("exception occurred when the frontend connection's state were recorded", ex);
        }
    }

    public void remove(AbstractConnection connection) {
        if (usageStats == null || !(connection instanceof FrontendConnection))
            return;
        WorkStat stat = usageStats.remove(connection);
        if (stat != null)
            stat.clear();
    }

    public Map<FrontendConnection, String[]> getActiveRatioStat() {
        Map<FrontendConnection, String[]> maps = Maps.newHashMap();
        if (usageStats == null)
            return maps;
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<FrontendConnection, WorkStat> s : usageStats.entrySet()) {
            maps.put(s.getKey(), s.getValue().getActiveRatioStat(currentTime));
        }
        return maps;
    }

    // timed task
    public void clearStaleData() {
        try {
            if (!enable) return;
            long currentTime = System.currentTimeMillis();
            for (WorkStat w : usageStats.values()) {
                w.adjust(currentTime);
            }
        } catch (Exception e) {
            LOGGER.warn("clearStaleData() exception：{}", e);
        }
    }

    private WorkStat newWorkStat(long time) {
        WorkStat stat = new WorkStat();
        stat.init(time);
        return stat;
    }

    public class WorkStat {
        private LinkedList<Time2> list;
        private Object mutex;
        private static final long LAST_STAT_30 = 30 * 1000; // 30s
        private static final long LAST_STAT_60 = 60 * 1000; // 1min
        private static final long LAST_STAT_300 = 5 * 60 * 1000; // 5min

        private static final long LAST_STAT_MAX = LAST_STAT_300;

        public WorkStat() {
            this.list = new LinkedList<Time2>();
            this.mutex = this;
        }

        private void init(long time) {
            synchronized (mutex) {
                list.add(new ReadTime(time));
                list.add(new WriteTime(time));
            }
        }

        private void clear() {
            synchronized (mutex) {
                list.clear();
            }
        }

        public void readTime(long time) {
            synchronized (mutex) {
                if (CollectionUtil.isEmpty(list)) {
                    list.add(new ReadTime(time));
                } else {
                    if (list.getLast() instanceof WriteTime) {
                        list.add(new ReadTime(time));
                    } // else, (list.getLast() instanceof WriteTime) not record
                }
            }
        }

        public void writeTime(long time) {
            synchronized (mutex) {
                if (!CollectionUtil.isEmpty(list) && list.getLast() instanceof WriteTime) {
                    list.getLast().setTime0(time);
                } else {
                    list.add(new WriteTime(time));
                }
            }
        }

        private void adjust(long currentTime) {
            long recentTime = currentTime - LAST_STAT_MAX;
            synchronized (mutex) {
                final Predicate<Time2> predicate = t -> (t.getValue() < recentTime);
                Time2 last = list.getLast();
                list.removeIf(predicate);
                if (last instanceof ReadTime) {
                    list.add(last); // last Time2 is ReadTime, should not be removed
                }
            }
        }

        public String[] getActiveRatioStat(long currentTime) {
            List<Time2> list0;
            synchronized (mutex) {
                list0 = new LinkedList<>(list);
            }
            String stat30 = getActiveRatio(list0, currentTime, LAST_STAT_30);
            String stat60 = getActiveRatio(list0, currentTime, LAST_STAT_60);
            String stat300 = getActiveRatio(list0, currentTime, LAST_STAT_300);

            return new String[]{stat30, stat60, stat300};
        }

        private String getActiveRatio(List<Time2> list0, long currentTime, long lastStatTime) {
            long recentTime = currentTime - lastStatTime;
            List<Time2> lists = list0.stream().
                    filter(l -> l.getValue() >= recentTime && l.getValue() <= currentTime).
                    collect(Collectors.toList());

            if (CollectionUtil.isEmpty(lists))
                return 0 + "%";

            long usage = getUsageWay(lists, recentTime, currentTime);
            int percent = (int) (usage / ((float) lastStatTime) * 100);
            return percent + "%";
        }

        private long getUsageWay(List<Time2> ls, long recentTime, long currentTime) {
            long usageTime = 0;
            for (int i = 0; i < ls.size(); i++) {
                if (i == 0 && ls.get(i) instanceof WriteTime)
                    usageTime += (ls.get(i).getValue() - recentTime);

                if (ls.get(i) instanceof ReadTime) {
                    if ((i + 1) <= ls.size() - 1) {
                        usageTime += ls.get(i + 1).getValue() - ls.get(i).getValue();
                        i++;
                    } else {
                        usageTime += currentTime - ls.get(i).getValue();
                    }
                }
            }
            return usageTime;
        }
    }

    private class ReadTime extends Time2 {
        ReadTime(long vlong) {
            super(vlong);
        }
    }

    private class WriteTime extends Time2 {
        WriteTime(long vlong) {
            super(vlong);
        }
    }

    private abstract class Time2 {
        long time0;

        Time2(long time0) {
            this.time0 = time0;
        }

        long getValue() {
            return time0;
        }

        public void setTime0(long time0) {
            this.time0 = time0;
        }
    }
}
