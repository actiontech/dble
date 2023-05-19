/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.btrace.provider.StatisticProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.SqlLog;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.UsageData;
import com.actiontech.dble.statistic.sql.UsageDataBlock;
import com.actiontech.dble.util.DateUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatisticCf {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCf.class);
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    public static class OnOff {
        public OnOff() {
        }

        public static void execute(ManagerService service, boolean isOn) {
            LOCK.writeLock().lock();
            try {
                StatisticProvider.onOffStatistic();
                String onOffStatus = isOn ? "enable" : "disable";
                try {
                    WriteDynamicBootstrap.getInstance().changeValue("enableStatistic", isOn ? "1" : "0");
                } catch (Exception ex) {
                    LOGGER.warn("rollback enableStatistic failed, exception：", ex);
                    service.writeErrMessage(ErrorCode.ER_YES, onOffStatus + " enableStatistic failed");
                    return;
                }
                StatisticManager.getInstance().setEnable(isOn);
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.write(service.getConnection());
            } finally {
                LOCK.writeLock().unlock();
            }
        }
    }

    public static class AnalysisOnOff {
        public AnalysisOnOff() {
        }

        public static void execute(ManagerService service, boolean isOn) {
            LOCK.writeLock().lock();
            try {
                StatisticProvider.onOffStatistic();
                String onOffStatus = isOn ? "enable" : "disable";
                try {
                    WriteDynamicBootstrap.getInstance().changeValue("enableStatisticAnalysis", isOn ? "1" : "0");
                } catch (Exception ex) {
                    LOGGER.warn("rollback enableStatisticAnalysis failed, exception：", ex);
                    service.writeErrMessage(ErrorCode.ER_YES, onOffStatus + " enableStatisticAnalysis failed");
                    return;
                }
                StatisticManager.getInstance().setEnableAnalysis(isOn);
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.write(service.getConnection());
            } finally {
                LOCK.writeLock().unlock();
            }
        }
    }

    public static class SamplingSwitch {
        public SamplingSwitch() {
        }

        public static void execute(ManagerService service, int samplingRate) {
            if (samplingRate < 0 || samplingRate > 100) {
                service.writeErrMessage(ErrorCode.ER_YES, "value of samplingRate is incorrect, the value is integer between 0 and 100.");
                return;
            }
            LOCK.writeLock().lock();
            try {
                try {
                    WriteDynamicBootstrap.getInstance().changeValue("samplingRate", samplingRate + "");
                } catch (Exception ex) {
                    LOGGER.warn("write samplingRate error", ex);
                    service.writeErrMessage(ErrorCode.ER_YES, "fail to reload samplingRate");
                    return;
                }
                StatisticManager.getInstance().setSamplingRate(samplingRate);
                service.writeOkPacket();
            } finally {
                LOCK.writeLock().unlock();
            }
        }
    }

    public static class SetTableMaxSize {
        // reload @@statistic_table_size = 96
        //reload @@statistic_table_size = 96 where table=table1;
        //reload @@statistic_table_size = 96 where table in(schema1.table1,...)
        static final Pattern PATTERN_IN = Pattern.compile("^([^\\s]+)(\\s+where\\s+table\\s+in\\s*\\(([^\\s]+)\\))*", Pattern.CASE_INSENSITIVE);
        static final Pattern PATTERN_EQUAL = Pattern.compile("^([^\\s]+)(\\s+where\\s+table\\s*=\\s*'([^\\s]+)')*", Pattern.CASE_INSENSITIVE);
        static final Map<String, String> STATISTIC_TABLES = new HashMap<>(8);

        static {
            STATISTIC_TABLES.put(AssociateTablesByEntryByUser.TABLE_NAME, "associateTablesByEntryByUserTableSize");
            STATISTIC_TABLES.put(FrontendByBackendByEntryByUser.TABLE_NAME, "frontendByBackendByEntryByUserTableSize");
            STATISTIC_TABLES.put(TableByUserByEntry.TABLE_NAME, "tableByUserByEntryTableSize");
            STATISTIC_TABLES.put(SqlLog.TABLE_NAME, "sqlLogTableSize");
        }

        public static void execute(ManagerService service, String value) {
            LOCK.writeLock().lock();
            try {
                StatisticProvider.updateTableMaxSize();

                Matcher matcher1 = PATTERN_IN.matcher(value);
                Matcher matcher2 = PATTERN_EQUAL.matcher(value);
                int size = 0;
                boolean haveCondition = false;
                String tableStr = null;
                try {
                    if (matcher1.matches()) {
                        size = Integer.parseInt(matcher1.group(1));
                        if ((tableStr = matcher1.group(3)) != null) {
                            haveCondition = true;
                        }
                    } else if (matcher2.matches()) {
                        size = Integer.parseInt(matcher2.group(1));
                        if ((tableStr = matcher2.group(3)) != null) {
                            haveCondition = true;
                        }
                    } else {
                        service.writeErrMessage(ErrorCode.ER_YES, "Usage: reload @@statistic_table_size = 1024 [where table='?' | where table in (dble_information.tableA,...)]");
                        return;
                    }
                } catch (NumberFormatException e) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "tableSize setting is not correct");
                    return;
                }
                if (size < 1) {
                    service.writeErrMessage(ErrorCode.ER_YES, "tableSize must be greater than 0");
                    return;
                }

                Set<String> tables;
                if (haveCondition) {
                    tables = new HashSet<>();
                    String[] tableArray = StringUtil.removeApostropheOrBackQuote(tableStr).split(",");
                    for (String table : tableArray) {
                        String[] arr = table.split("\\.");
                        if (arr.length == 1) {
                            String tableTmp;
                            if (!STATISTIC_TABLES.containsKey((tableTmp = StringUtil.removeApostropheOrBackQuote(arr[0]).toLowerCase()))) {
                                service.writeErrMessage(ErrorCode.ER_YES, "Table `" + ManagerSchemaInfo.SCHEMA_NAME + "`.`" + arr[0] + "` don't belong to statistic tables");
                                return;
                            } else {
                                tables.add(tableTmp);
                            }
                        } else if (arr.length == 2) {
                            String tableTmp;
                            if (!StringUtil.removeApostropheOrBackQuote(arr[0]).equalsIgnoreCase(ManagerSchemaInfo.SCHEMA_NAME) ||
                                    !STATISTIC_TABLES.containsKey((tableTmp = StringUtil.removeApostropheOrBackQuote(arr[1])))) {
                                service.writeErrMessage(ErrorCode.ER_YES, "Table `" + arr[0] + "`.`" + arr[1] + "` don't belong to statistic tables");
                                return;
                            } else {
                                tables.add(tableTmp);
                            }
                        } else {
                            service.writeErrMessage(ErrorCode.ER_YES, "Please check table name is correct");
                            return;
                        }
                    }
                } else {
                    tables = new HashSet<>(STATISTIC_TABLES.keySet());
                }

                try {
                    List<Pair<String, String>> props = new ArrayList<>();
                    for (String t : tables) {
                        props.add(new Pair<>(STATISTIC_TABLES.get(t), String.valueOf(size)));
                    }
                    WriteDynamicBootstrap.getInstance().changeValue(props);
                } catch (Exception ex) {
                    LOGGER.warn("set statistic table size failed", ex);
                    service.writeErrMessage(ErrorCode.ER_YES, "set statistic table size failed");
                    return;
                }
                for (String t : tables) {
                    switch (t.toLowerCase()) {
                        case AssociateTablesByEntryByUser.TABLE_NAME:
                            StatisticManager.getInstance().setAssociateTablesByEntryByUserTableSize(size);
                            break;
                        case FrontendByBackendByEntryByUser.TABLE_NAME:
                            StatisticManager.getInstance().setFrontendByBackendByEntryByUserTableSize(size);
                            break;
                        case TableByUserByEntry.TABLE_NAME:
                            StatisticManager.getInstance().setTableByUserByEntryTableSize(size);
                            break;
                        case SqlLog.TABLE_NAME:
                            StatisticManager.getInstance().setSqlLogSize(size);
                            break;
                        default:
                            break;
                    }
                    /*Method method = StatisticManager.getInstance().getClass().getMethod("",new Class[]{int.class});
                       method.invoke(StatisticManager.getInstance(),size);*/
                }

                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.write(service.getConnection());
            } finally {
                LOCK.writeLock().unlock();
            }
        }
    }

    public static class Show {

        public Show() {
        }

        private static final int FIELD_COUNT = 2;
        private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
        private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
        private static final EOFPacket EOF = new EOFPacket();

        static {
            int i = 0;
            byte packetId = 0;
            HEADER.setPacketId(++packetId);

            FIELDS[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[i++].setPacketId(++packetId);

            FIELDS[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[i].setPacketId(++packetId);

            EOF.setPacketId(++packetId);
        }

        public static void execute(ManagerService service) {
            LOCK.readLock().lock();
            try {
                StatisticProvider.showStatistic();
                ByteBuffer buffer = service.allocate();
                // write header
                buffer = HEADER.write(buffer, service, true);

                // write fields
                for (FieldPacket field : FIELDS) {
                    buffer = field.write(buffer, service, true);
                }

                // write eof
                buffer = EOF.write(buffer, service, true);

                // write rows
                byte packetId = EOF.getPacketId();

                RowDataPacket row1 = new RowDataPacket(FIELD_COUNT);
                row1.add(StringUtil.encode("statistic", service.getCharset().getResults()));
                row1.add(StringUtil.encode(StatisticManager.getInstance().isEnable() ? "ON" : "OFF", service.getCharset().getResults()));
                row1.setPacketId(++packetId);
                buffer = row1.write(buffer, service, true);

                RowDataPacket row8 = new RowDataPacket(FIELD_COUNT);
                row8.add(StringUtil.encode("statisticAnalysis", service.getCharset().getResults()));
                row8.add(StringUtil.encode(StatisticManager.getInstance().isEnableAnalysis() ? "ON" : "OFF", service.getCharset().getResults()));
                row8.setPacketId(++packetId);
                buffer = row8.write(buffer, service, true);

                RowDataPacket row2 = new RowDataPacket(FIELD_COUNT);
                row2.add(StringUtil.encode("associateTablesByEntryByUserTableSize", service.getCharset().getResults()));
                row2.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getAssociateTablesByEntryByUserTableSize()), service.getCharset().getResults()));
                row2.setPacketId(++packetId);
                buffer = row2.write(buffer, service, true);

                RowDataPacket row3 = new RowDataPacket(FIELD_COUNT);
                row3.add(StringUtil.encode("frontendByBackendByEntryByUserTableSize", service.getCharset().getResults()));
                row3.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getFrontendByBackendByEntryByUserTableSize()), service.getCharset().getResults()));
                row3.setPacketId(++packetId);
                buffer = row3.write(buffer, service, true);

                RowDataPacket row4 = new RowDataPacket(FIELD_COUNT);
                row4.add(StringUtil.encode("tableByUserByEntryTableSize", service.getCharset().getResults()));
                row4.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getTableByUserByEntryTableSize()), service.getCharset().getResults()));
                row4.setPacketId(++packetId);
                buffer = row4.write(buffer, service, true);

                RowDataPacket row5 = new RowDataPacket(FIELD_COUNT);
                row5.add(StringUtil.encode("sqlLogTableSize", service.getCharset().getResults()));
                row5.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getSqlLogSize()), service.getCharset().getResults()));
                row5.setPacketId(++packetId);
                buffer = row5.write(buffer, service, true);

                RowDataPacket row6 = new RowDataPacket(FIELD_COUNT);
                row6.add(StringUtil.encode("samplingRate", service.getCharset().getResults()));
                row6.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getSamplingRate()), service.getCharset().getResults()));
                row6.setPacketId(++packetId);
                buffer = row6.write(buffer, service, true);

                RowDataPacket row7 = new RowDataPacket(FIELD_COUNT);
                row7.add(StringUtil.encode("queueMonitor", service.getCharset().getResults()));
                row7.add(StringUtil.encode(StatisticManager.getInstance().isMonitoring() ? "monitoring" : "-", service.getCharset().getResults()));
                row7.setPacketId(++packetId);
                buffer = row7.write(buffer, service, true);

                // write last eof
                EOFRowPacket lastEof = new EOFRowPacket();
                lastEof.setPacketId(++packetId);

                lastEof.write(buffer, service);
            } finally {
                LOCK.readLock().unlock();
            }
        }
    }

    public static class Queue {
        private static final int FIELD_COUNT = 2;
        private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
        private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
        private static final EOFPacket EOF = new EOFPacket();
        private static final Pattern PATTERN = Pattern.compile("(\\s+observeTime\\s*=\\s*(([^\\s])+)(\\s+and\\s+intervalTime\\s*=\\s*([^']+))?)?", Pattern.CASE_INSENSITIVE);

        private static final ImmutableMap<String, TimeUnit> TIME_SUFFIXES =
                ImmutableMap.<String, TimeUnit>builder().
                        put("s", TimeUnit.SECONDS).
                        put("m", TimeUnit.MINUTES).
                        put("min", TimeUnit.MINUTES).
                        put("h", TimeUnit.HOURS).
                        build();

        static {
            int i = 0;
            byte packetId = 0;
            HEADER.setPacketId(++packetId);

            FIELDS[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[i++].setPacketId(++packetId);

            FIELDS[i] = PacketUtil.getField("USAGE", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[i].setPacketId(++packetId);

            EOF.setPacketId(++packetId);
        }

        // start @@statistic_queue_monitor observeTime = 0 and intervalTime = 0;
        public static void start(ManagerService service, String stmt, int offset) {
            String option = stmt.substring(offset);
            Matcher ma = PATTERN.matcher(option);
            if (!ma.matches()) {
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "The sql does not match: start @@statistic_queue_monitor observeTime = ? and intervalTime = ?");
                return;
            }
            long observeTime = 60000L;
            long intervalTime = 5000L;
            try {
                if (null != ma.group(2)) {
                    observeTime = JavaUtils.timeStringAsMs(ma.group(2), TIME_SUFFIXES);
                }
                if (null != ma.group(5)) {
                    intervalTime = JavaUtils.timeStringAsMs(ma.group(5), TIME_SUFFIXES);
                }
                if (observeTime < intervalTime) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Rule: must be a positive integer, observeTime > intervalTime, Unit: (s,m/min,h)");
                return;
            }
            if (!StatisticManager.getInstance().isEnable() && StatisticManager.getInstance().getSamplingRate() == 0) {
                service.writeErrMessage(ErrorCode.ER_YES, "Statistic is disabled and samplingRate value is 0");
                return;
            }
            if (StatisticManager.getInstance().isMonitoring()) {
                service.writeErrMessage(ErrorCode.ER_YES, "In the monitoring..., can use 'stop @@statistic_queue_monitor' to interrupt monitoring");
                return;
            }

            StatisticManager.getInstance().resetUsageData();
            Timer timer = StatisticManager.getInstance().getQueueMonitor();
            if (null == timer) {
                service.writeErrMessage(ErrorCode.ER_YES, "Check the sql statistics is disabled or samplingRate value is 0");
                return;
            }
            timer.schedule(
                    new MonitorTask(
                            observeTime,
                            intervalTime),
                    0,
                    intervalTime);
            service.writeOkPacket();
        }

        // stop @@statistic_queue_monitor;
        public static void stop(ManagerService service) {
            StatisticManager.getInstance().cancelMonitoring();
            service.writeOkPacket();
        }

        // show @@statistic_queue.usage;
        public static void show(ManagerService service) {
            ByteBuffer buffer = service.allocate();
            buffer = HEADER.write(buffer, service, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, service, true);
            }
            buffer = EOF.write(buffer, service, true);
            byte packetId = EOF.getPacketId();
            RowDataPacket row;
            UsageData usageData;

            for (UsageDataBlock block : StatisticManager.getInstance().getUsageData()) {
                if ((usageData = block.get()) != null) {
                    row = new RowDataPacket(FIELD_COUNT);
                    row.add(StringUtil.encode(usageData.getDataTime(), service.getCharset().getResults()));
                    row.add(StringUtil.encode(usageData.getUsage(), service.getCharset().getResults()));
                    row.setPacketId(++packetId);
                    buffer = row.write(buffer, service, true);
                }
            }
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(++packetId);
            lastEof.write(buffer, service);
        }

        public static void drop(ManagerService service) {
            StatisticManager.getInstance().resetUsageData();
            service.writeOkPacket();
        }

        public static class MonitorTask extends TimerTask {
            private static final DecimalFormat DF = new DecimalFormat("0.00%");
            private double queueSize = SystemConfig.getInstance().getStatisticQueueSize();
            private long interval;
            private long observe;
            private long count = 0L;

            public MonitorTask(long observe, long interval) {
                super();
                this.interval = interval;
                this.observe = observe;
            }

            @Override
            public void run() {
                StatisticManager.getInstance().
                        getUsageData().add(new UsageDataBlock(DateUtil.parseStr(System.currentTimeMillis(), DateUtil.DEFAULT_DATE_PATTERN),
                        DF.format(1 - (StatisticManager.getInstance().getDisruptorRemaining() / queueSize))));
                count += interval;
                if (count > observe) {
                    StatisticManager.getInstance().cancelMonitoring();
                    return;
                }
            }
        }
    }
}
