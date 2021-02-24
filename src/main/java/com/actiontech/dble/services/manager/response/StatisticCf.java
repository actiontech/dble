package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.btrace.provider.StatisticProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
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
            try {
                LOCK.writeLock().lock();
                StatisticProvider.onOffStatistic();
                String onOffStatus = isOn ? "enable" : "disable";
                try {
                    WriteDynamicBootstrap.getInstance().changeValue("enableStatistic", isOn ? "0" : "1");
                } catch (Exception ex) {
                    LOGGER.warn("rollback enableStatistic failed, exception：{}", ex);
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

    public static class SetTableMaxSize {
        // reload @@statistic_table_size = 96
        //reload @@statistic_table_size = 96 where table=table1;
        //reload @@statistic_table_size = 96 where table in(schema1.table1,...)
        public static final Pattern PATTERN_IN = Pattern.compile("^\\s*(\\d+)\\s*(where\\s+table\\s+in\\s*\\(([^\\s]+)\\))*", Pattern.CASE_INSENSITIVE);
        public static final Pattern PATTERN_EQUAL = Pattern.compile("^\\s*(\\d+)\\s*(where\\s+table\\s*=\\s*'([^\\s]+)')*", Pattern.CASE_INSENSITIVE);
        public static final Map<String, String> STATISTIC_TABLES = new HashMap(3);

        static {
            STATISTIC_TABLES.put(AssociateTablesByEntryByUser.TABLE_NAME, "associateTablesByEntryByUserTableSize");
            STATISTIC_TABLES.put(FrontendByBackendByEntryByUser.TABLE_NAME, "frontendByBackendByEntryByUserTableSize");
            STATISTIC_TABLES.put(TableByUserByEntry.TABLE_NAME, "tableByUserByEntryTableSize");
        }

        public static void execute(ManagerService service, String value) {
            try {
                LOCK.writeLock().lock();
                StatisticProvider.updateTableMaxSize();

                Matcher matcher1 = PATTERN_IN.matcher(value);
                Matcher matcher2 = PATTERN_EQUAL.matcher(value);
                int size = 0;
                boolean haveCondition = false;
                String tableStr = null;

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
                    service.writeErrMessage(ErrorCode.ER_YES, "Usage: reload @@statistic_table_size = 1000 [where table='?' | where table in (dble_information.tableA,...)]");
                    return;
                }

                Set<String> tables;
                if (haveCondition) {
                    tables = new HashSet<>();
                    String[] tableArray = tableStr.split(",");
                    for (String table : tableArray) {
                        String[] arr = table.split("\\.");
                        if (arr.length == 1) {
                            if (!STATISTIC_TABLES.keySet().contains(StringUtil.removeApostropheOrBackQuote(arr[0]).toLowerCase())) {
                                service.writeErrMessage(ErrorCode.ER_YES, "Table `" + ManagerSchemaInfo.SCHEMA_NAME + "`.`" + arr[0] + "` don't belong to statistic tables");
                                return;
                            } else {
                                tables.add(table);
                            }
                        } else if (arr.length == 2) {
                            if (!StringUtil.removeApostropheOrBackQuote(arr[0]).toLowerCase().equals(ManagerSchemaInfo.SCHEMA_NAME) ||
                                    !STATISTIC_TABLES.keySet().contains(StringUtil.removeApostropheOrBackQuote(arr[1]))) {
                                service.writeErrMessage(ErrorCode.ER_YES, "Table `" + arr[0] + "`.`" + arr[1] + "` don't belong to statistic tables");
                                return;
                            } else {
                                tables.add(table);
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
                    if (t.toLowerCase().equals(AssociateTablesByEntryByUser.TABLE_NAME)) {
                        StatisticManager.getInstance().setAssociateTablesByEntryByUserTableSize(size);
                    } else if (t.toLowerCase().equals(FrontendByBackendByEntryByUser.TABLE_NAME)) {
                        StatisticManager.getInstance().setFrontendByBackendByEntryByUserTableSize(size);
                    } else if (t.toLowerCase().equals(TableByUserByEntry.TABLE_NAME)) {
                        StatisticManager.getInstance().setTableByUserByEntryTableSize(size);
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
            try {
                LOCK.readLock().lock();
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

                // write last eof
                EOFRowPacket lastEof = new EOFRowPacket();
                lastEof.setPacketId(++packetId);

                lastEof.write(buffer, service);
            } finally {
                LOCK.readLock().unlock();
            }
        }
    }

}
