/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.stat;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class FrontActiveRatioStat {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontActiveRatioStat.class);

    private static final int COMPRESSION_INTERVAL = 200; // ms
    public static final long INTERVAL = COMPRESSION_INTERVAL;

    private static final int LAST_STAT_30S = 30 * 1000; // 30s
    private static final int LAST_STAT_1MIN = 60 * 1000; // 1min
    private static final int LAST_STAT_5MIN = 5 * 60 * 1000; // 5min

    public static final int CACHE_30S_SIZE = LAST_STAT_30S / COMPRESSION_INTERVAL;
    public static final int CACHE_1MIN_SIZE = LAST_STAT_1MIN / COMPRESSION_INTERVAL;
    public static final int CACHE_5MIN_SIZE = LAST_STAT_5MIN / COMPRESSION_INTERVAL; // best divisible
    private Map<FrontendConnection, WorkStat> usageStats;
    private static final FrontActiveRatioStat INSTANCE = new FrontActiveRatioStat();

    private volatile long lastCompressTime = 0;

    private boolean enable = false;

    public FrontActiveRatioStat() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1)
            return;
        enable = SystemConfig.getInstance().getEnableSessionActiveRatioStat() == 1;
        if (enable) {
            usageStats = new ConcurrentHashMap<>();
        }
    }

    public static FrontActiveRatioStat getInstance() {
        return INSTANCE;
    }

    public boolean isEnable() {
        return enable;
    }

    public void register(AbstractConnection connection, long initTime) {
        if (usageStats == null || !(connection instanceof FrontendConnection))
            return;
        usageStats.put((FrontendConnection) connection, createWorkStat(initTime));
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

    public void compress() {
        try {
            if (!enable) return;
            long currentCompressTime;
            if (lastCompressTime == 0) {
                currentCompressTime = System.currentTimeMillis();
            } else {
                currentCompressTime = lastCompressTime + COMPRESSION_INTERVAL;
            }
            while (currentCompressTime <= System.currentTimeMillis()) {
                for (WorkStat w : usageStats.values()) {
                    w.compressToStatic(currentCompressTime);
                }
                lastCompressTime = currentCompressTime;
                currentCompressTime = lastCompressTime + COMPRESSION_INTERVAL;
            }
        } catch (Exception e) {
            LOGGER.warn("compress() exception：{}", e);
        }
    }

    public Map<FrontendConnection, String[]> getActiveRatioStat() {
        Map<FrontendConnection, String[]> maps = Maps.newHashMap();
        try {
            if (usageStats == null)
                return maps;
            long currentTime = System.currentTimeMillis();
            for (Map.Entry<FrontendConnection, WorkStat> s : usageStats.entrySet()) {
                String[] activeRatioStats = s.getValue().getActiveRatioStat(currentTime);
                if (activeRatioStats != null)
                    maps.put(s.getKey(), activeRatioStats);
            }
        } catch (Exception e) {
            LOGGER.warn("getActiveRatioStat() exception：{}", e);
        }
        return maps;
    }

    private WorkStat createWorkStat(long time) {
        WorkStat stat = new WorkStat(time);
        return stat;
    }

    public static class WorkStat {
        private LinkedList<PointTime> dynamicArray; // storage PointTime
        private StaticArray staticArray;
        private PointTime lastPointTimeInStatic = null; // in static, last point
        private long lastStaticTime = 0L; // last compressed time
        private Object mutex;

        public WorkStat(long initTime) {
            this.dynamicArray = new LinkedList<PointTime>();
            this.staticArray = new StaticArray(CACHE_5MIN_SIZE);
            this.mutex = this;
            this.dynamicArray.add(new ReadTime(initTime));
            this.dynamicArray.add(new WriteTime(initTime));
        }

        public void readTime(long time) {
            synchronized (mutex) {
                if (isClose()) return;
                if (CollectionUtil.isEmpty(dynamicArray)) {
                    dynamicArray.add(new ReadTime(time));
                } else {
                    /**
                     * [Enhanced handling]
                     * When current readTime is the same as the last WritePoint's time, the last WritePoint is removed directly
                     */
                    if (dynamicArray.getLast() instanceof WriteTime) {
                        if (dynamicArray.getLast().getValue() == time) {
                            dynamicArray.removeLast();
                        } else {
                            dynamicArray.add(new ReadTime(time));
                        }
                    } // else, list.getLast() instanceof ReadTime. In theory there is no this scenario
                }
            }
        }

        public void writeTime(long time) {
            synchronized (mutex) {
                if (isClose()) return;
                if (CollectionUtil.isEmpty(dynamicArray)) {
                    dynamicArray.add(new WriteTime(time));
                } else {
                    /**
                     * [Enhanced handling]
                     * In scenarios (respond multiple times to the frontend): 1、multiple statements 2、result set is large
                     * Will appear ReadPoint:WritePoint=1:N; So just update the time of the first WritePoint
                     */
                    if (dynamicArray.getLast() instanceof WriteTime) {
                        dynamicArray.getLast().setTime(time);
                    } else {
                        dynamicArray.add(new WriteTime(time));
                    }
                }
            }
        }

        private void clear() {
            synchronized (mutex) {
                if (isClose()) return;
                dynamicArray.clear();
                dynamicArray = null;
                staticArray.clear();
                staticArray = null;
                lastPointTimeInStatic = null;
                lastStaticTime = 0L;
            }
        }

        private boolean isClose() {
            return staticArray == null || dynamicArray == null;
        }

        public String[] getActiveRatioStat(long currentTime) {
            LinkedList<PointTime> dynamicArray0;
            int[] staticArray0;
            long lastStaticTime0;
            synchronized (mutex) {
                if (isClose()) return null;
                dynamicArray0 = new LinkedList<>(dynamicArray);
                staticArray0 = staticArray.getSort();
                lastStaticTime0 = this.getLastStaticTime() == 0 ? currentTime - COMPRESSION_INTERVAL : this.getLastStaticTime(); // query time, greater than the first compression time
            }

            /**
             * Calculate the activeTimeCount in dynamicArray, interval [lastStaticTime, currentTime]
             */
            Object[] dRes = calcActiveTimeCountInDynamic(dynamicArray0, lastStaticTime0, currentTime);
            int activeTimeCountInDynamic = (int) dRes[0];

            /**
             * Calculate the activeTimeCount in staticArray, interval last 30s/1min/5 min.
             */
            int[] sRes = calcActiveTimeCountInStatic(staticArray0, currentTime, dRes[1], lastStaticTime0);

            // calculate percent
            String stat30s = calcPercent(sRes[0] + activeTimeCountInDynamic, LAST_STAT_30S);
            String stat1min = calcPercent(sRes[1] + activeTimeCountInDynamic, LAST_STAT_1MIN);
            String stat5min = calcPercent(sRes[2] + activeTimeCountInDynamic, LAST_STAT_5MIN);
            return new String[]{stat30s, stat1min, stat5min};
        }

        /**
         * Calculate the activeTimeCount in staticArray, interval last 30s/1min/5min.
         * <p>
         * logic:
         * 30s/1min/5min, activeTimeCount = The last ${CACHE_30S_SIZE/CACHE_1MIN_SIZE/CACHE_5MIN_SIZE} values in the staticArray are added
         * <p>
         * Multi-package processing, see method: getMissingTime()
         * Edge processing, see param: edgeActiveTimeCountCount
         */
        private int[] calcActiveTimeCountInStatic(int[] array, long currentTime, Object firstWritePointInDynamic, long lastcompressTime0) {
            int size = array.length;
            if (size == 0)
                return new int[]{0, 0, 0};

            int edgeNum = (int) Math.round((double) (currentTime - lastcompressTime0) / COMPRESSION_INTERVAL);
            int missingTime = getMissingTime(firstWritePointInDynamic, lastcompressTime0);

            // 30s
            int indexOfLast30s = size - CACHE_30S_SIZE;
            int[] resOfLast30s = calcCountInStatic(array, indexOfLast30s, size, edgeNum);
            int activeTimeCountOfLast30s = resOfLast30s[0] + missingTime;

            // 1min
            int indexOfLast1min = size - CACHE_1MIN_SIZE;
            int[] resOfLast1min = calcCountInStatic(array, indexOfLast1min, indexOfLast30s, edgeNum);
            int activeTimeCountOfLast1min = resOfLast1min[0] + activeTimeCountOfLast30s;

            // 5min
            int indexOfLast5min = 0; // size > CACHE_5MIN_SIZE ? (size - CACHE_5MIN_SIZE) : 0;
            int[] resOfLast5min = calcCountInStatic(array, indexOfLast5min, indexOfLast1min, edgeNum);
            int activeTimeCountOfLast5min = resOfLast5min[0] + activeTimeCountOfLast1min;

            // edge processing
            activeTimeCountOfLast5min -= resOfLast5min[1];
            activeTimeCountOfLast1min -= resOfLast1min[1];
            activeTimeCountOfLast30s -= resOfLast30s[1];

            /**
             * sum of the activeTime in 30s/1min/5min in staticArray
             */
            return new int[]{activeTimeCountOfLast30s, activeTimeCountOfLast1min, activeTimeCountOfLast5min};
        }

        private int[] calcCountInStatic(int[] array, int fromIndex, int toIndex, int edgeNum) {
            int activeTimeCount = 0;
            int edgeActiveTimeCountCount = 0; // Used to cull edge data
            for (int i = fromIndex; i < toIndex; i++) {
                activeTimeCount += array[i];
                if (i == fromIndex + (edgeNum - 1)) {
                    edgeActiveTimeCountCount = activeTimeCount;
                }
            }
            return new int[]{activeTimeCount, edgeActiveTimeCountCount};
        }

        private String calcPercent(int activeTime, int lastStatTime) {
            if (activeTime >= lastStatTime)
                return "100%";
            int percent = (int) (activeTime / ((float) lastStatTime) * 100);
            return percent + "%";
        }

        /**
         * Triggered by Timed Task
         * Calculate last ${COMPRESSION_INTERVAL} in dynamicArray, and compress to staticArray
         * <p>
         * logic:
         * 1.in last ${COMPRESSION_INTERVAL}, if no data:
         * 2.1. if lastPointTimeInStatic is WriteTime, so activeTimeCount=0
         * 2.2. if lastPointTimeInStatic is ReadTime, so activeTimeCount=${COMPRESSION_INTERVAL}
         * <p>
         * 2. if there is data, To calcActiveTimeCountInDynamic()
         * <p>
         * 3. clear dynamicArray
         * 4. add currentActiveTimeCount to staticArray
         */
        private void compressToStatic(long currentCompressTime) {
            synchronized (mutex) {
                if (isClose()) return;
                this.initLastStaticTime(currentCompressTime);
                int currentActiveTimeCount;
                int index = lastObsoleteIndex(dynamicArray, currentCompressTime);
                List<PointTime> calcLists = dynamicArray.subList(0, index + 1);

                if (CollectionUtil.isEmpty(calcLists)) {
                    if (!lastPointTypeIsWriteInStatic()) {
                        currentActiveTimeCount = COMPRESSION_INTERVAL;
                    } else {
                        currentActiveTimeCount = 0;
                    }
                } else {
                    /**
                     * Calculate the activeTimeCount in dynamicArray, interval [getLastStaticTime(), currentCompressTime]
                     */
                    Object[] dRes = calcActiveTimeCountInDynamic(calcLists, getLastStaticTime(), currentCompressTime);
                    tryMakeUpMissingTimeInStatic(dRes[1], getLastStaticTime());
                    currentActiveTimeCount = (int) dRes[0];
                    lastPointTimeInStatic = calcLists.get(calcLists.size() - 1);
                    // clean up calcLists in dynamicArray
                    dynamicArray.removeAll(new HashSet<>(calcLists)); // list.removeAll(set) is moving much faster than list.removeAll(list)
                }
                // add to staticArray
                staticArray.add(currentActiveTimeCount);
                lastStaticTime = currentCompressTime;
            }
        }

        /**
         * make up MissingTime
         */
        private void tryMakeUpMissingTimeInStatic(Object firstWritePointInDynamic, long lastCompressTime0) {
            int missingTime = getMissingTime(firstWritePointInDynamic, lastCompressTime0);
            if (missingTime > 0) {
                staticArray.cumulativeMissTime(missingTime);
            }
        }

        /**
         * Calculate the activeTimeCount in dynamicArray
         * <p>
         * logic:
         * the calculated interval for single activeTime:[ReadTime,WriteTime]
         * boundary processing logic:
         * 1. if the first PointTime is WriteTime, activeTime:[${lTime},WriteTime]
         * 2. if the first PointTime is ReadTime, activeTime:[ReadTime,${rTime}]
         * <p>
         * activeTimeCount=activeTime1 + activeTime2 + ... + activeTimeN;
         */
        private Object[] calcActiveTimeCountInDynamic(List<PointTime> ls, long lTime, long rTime) {
            int activeTimeCount = 0;
            WriteTime firstWritePointInDynamic = null;
            for (int i = 0; i < ls.size(); i++) {
                if (i == 0 && ls.get(i) instanceof WriteTime) {
                    activeTimeCount += (int) (ls.get(i).getValue() - lTime);
                    firstWritePointInDynamic = (WriteTime) ls.get(i);
                }

                if (ls.get(i) instanceof ReadTime) {
                    if ((i + 1) <= ls.size() - 1) {
                        activeTimeCount += (int) (ls.get(i + 1).getValue() - ls.get(i).getValue());
                        i++;
                    } else {
                        activeTimeCount += (int) (rTime - ls.get(i).getValue());
                    }
                }
            }
            if (activeTimeCount > COMPRESSION_INTERVAL)
                activeTimeCount = COMPRESSION_INTERVAL;

            return new Object[]{activeTimeCount, firstWritePointInDynamic};
        }

        /**
         * Calculate the missingTime in multi-package scenario(such as: 1、multiple statements 2、result set is large)
         * <p>
         * logic:
         * the PointTime type of the last compression was WritePoint, and the first PointTime type of type in the dynamicArray is WritePoint.
         * missingTime: (lastCompressTime - lastPointTimeInStatic)
         */
        private int getMissingTime(Object firstWritePointInDynamic, long lastCompressTime0) {
            if (firstWritePointInDynamic != null && lastPointTypeIsWriteInStatic()) {
                return (int) (lastCompressTime0 - lastPointTimeInStatic.getValue());
            }
            return 0;
        }

        /**
         * In static Array, last Point's Type is WritePoint?
         */
        private boolean lastPointTypeIsWriteInStatic() {
            return lastPointTimeInStatic == null ? true : lastPointTimeInStatic instanceof WriteTime;
        }

        /**
         * binary search
         */
        private int lastObsoleteIndex(LinkedList<PointTime> list0, long target) {
            int len = list0.size();
            int low = 0;
            int high = len - 1;

            int mid;
            while (low <= high) {
                mid = low + ((high - low) >> 1);
                if (list0.get(mid).getValue() < target) {
                    if (mid == len - 1 || list0.get(mid + 1).getValue() >= target) {
                        return mid;
                    } else {
                        low = mid + 1;
                    }
                } else {
                    high = mid - 1;
                }
            }
            return -1;
        }

        public void initLastStaticTime(long lastStaticTime0) {
            // init values
            if (this.lastStaticTime == 0L) {
                this.lastStaticTime = lastStaticTime0 - COMPRESSION_INTERVAL;
            }
        }

        public long getLastStaticTime() {
            return lastStaticTime;
        }
    }

    private static class ReadTime extends PointTime {
        ReadTime(long value) {
            super(value);
        }
    }

    private static class WriteTime extends PointTime {
        WriteTime(long value) {
            super(value);
        }
    }

    private abstract static class PointTime {
        volatile long time;

        PointTime(long time) {
            this.time = time;
        }

        long getValue() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }

    private static class StaticArray {
        private int capacity;
        private int[] array;  // circular array
        private int index = -1; // the current last data subscript position

        StaticArray(int capacity) { // CACHE_5MIN_SIZE
            this.capacity = capacity;
            this.array = new int[capacity];
        }

        private void add(int time) {
            if (index == -1) {
                index = 0;
            } else if (index + 1 > capacity - 1) {
                index = 0;
            } else {
                index++;
            }
            this.array[index] = time;
        }

        private void cumulativeMissTime(int value) {
            if (index == -1) {
                return;
            } else {
                int multiple = value / COMPRESSION_INTERVAL;
                int remainder = value % COMPRESSION_INTERVAL;
                if (remainder > 0) {
                    multiple++;
                }
                if (--multiple < 0) return;
                for (int i = multiple; i >= 0; i--) {
                    if (i == multiple) {
                        array[getPrefixIndex(i)] += remainder; // cumulative
                    } else {
                        array[getPrefixIndex(i)] = COMPRESSION_INTERVAL;
                    }
                }
            }
        }

        private int getPrefixIndex(int prefixNum) {
            if (prefixNum == 0) return index;
            int d = index - prefixNum;
            if (d < 0) {
                return capacity + d;
            } else {
                return index - prefixNum;
            }
        }

        private int[] getSort() {
            if (index == -1) return new int[0];
            if (index == capacity - 1) return array.clone();
            int[] newArray = new int[capacity];
            int aLen = capacity - (index + 1);
            int bLen = index + 1;
            System.arraycopy(array, index + 1, newArray, 0, aLen);
            System.arraycopy(array, 0, newArray, aLen, bLen);
            return newArray;
        }

        private void clear() {
            array = null;
            index = -1;
        }
    }
}
