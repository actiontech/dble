package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.statistic.backend.StatisticManager;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StatisticCf {

    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    public static class OnOff {
        public OnOff() {
        }

        public static void execute(ManagerService service, boolean isOn) {
            LOCK.writeLock().lock();
            try {
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

        public SetTableMaxSize() {
        }

        public static void execute(ManagerService service, String value) {
            LOCK.writeLock().lock();
            try {
                StatisticManager.getInstance().setStatisticTableSize(Integer.parseInt(value));
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.write(service.getConnection());
            } catch (NumberFormatException e) {
                service.writeErrMessage(ErrorCode.ER_YES, "tableMaxSize must be a number");
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
                row2.add(StringUtil.encode("statistic_table_size", service.getCharset().getResults()));
                row2.add(StringUtil.encode(String.valueOf(StatisticManager.getInstance().getStatisticTableSize()), service.getCharset().getResults()));
                row2.setPacketId(++packetId);
                buffer = row2.write(buffer, service, true);

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
