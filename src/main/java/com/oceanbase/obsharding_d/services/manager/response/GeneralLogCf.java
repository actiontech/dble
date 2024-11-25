/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.btrace.provider.GeneralProvider;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.log.FileUtils;
import com.oceanbase.obsharding_d.log.general.GeneralLogHelper;
import com.oceanbase.obsharding_d.log.general.GeneralLogProcessor;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.server.status.GeneralLog;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class GeneralLogCf {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLogCf.class);
    public static final String FILE_HEADER = "/FAKE_PATH/mysqld, Version: FAKE_VERSION. started with:\n" +
            "Tcp port: 3320  Unix socket: FAKE_SOCK\n" +
            "Time                 Id Command    Argument\n";
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private GeneralLogCf() {
    }

    // on/off
    public static class OnOffGeneralLog {
        public OnOffGeneralLog() {
        }

        public static void execute(ManagerService service, boolean isOn) {
            String onOffStatus = isOn ? "enable" : "disable";
            boolean isWrite = false;
            try {
                LOCK.writeLock().lock();
                GeneralProvider.onOffGeneralLog();
                WriteDynamicBootstrap.getInstance().changeValue("enableGeneralLog", isOn ? "1" : "0");
                isWrite = true;
                if (isOn) {
                    GeneralLog.getInstance().setEnableGeneralLog(true);
                    GeneralLogProcessor.getInstance().enable();
                } else {
                    GeneralLog.getInstance().setEnableGeneralLog(false);
                    GeneralLogProcessor.getInstance().disable();
                }
                LOGGER.info(service + " " + onOffStatus + " general_log success by manager");

                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.setMessage((onOffStatus + " general_log success").getBytes());
                ok.write(service.getConnection());
            } catch (Exception e) {
                String msg = onOffStatus + " general_log failed";
                LOGGER.warn(service + " " + msg + " exception：" + e);
                service.writeErrMessage(ErrorCode.ER_YES, msg);
                if (isWrite) {
                    try {
                        WriteDynamicBootstrap.getInstance().changeValue("enableGeneralLog", isOn ? "0" : "1");
                    } catch (Exception ex) {
                        LOGGER.warn("rollback enableGeneralLog failed, exception：{}", ex);
                    }
                }
                return;
            } finally {
                LOCK.writeLock().unlock();
            }
        }
    }

    // show
    public static class ShowGeneralLog {
        public ShowGeneralLog() {
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
                GeneralProvider.showGeneralLog();
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
                row1.add(StringUtil.encode("general_log", service.getCharset().getResults()));
                row1.add(StringUtil.encode(GeneralLog.getInstance().isEnableGeneralLog() ? "ON" : "OFF", service.getCharset().getResults()));
                row1.setPacketId(++packetId);
                buffer = row1.write(buffer, service, true);

                RowDataPacket row2 = new RowDataPacket(FIELD_COUNT);
                row2.add(StringUtil.encode("general_log_file", service.getCharset().getResults()));
                row2.add(StringUtil.encode(GeneralLog.getInstance().getGeneralLogFile(), service.getCharset().getResults()));
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

    // update path
    public static class ReloadGeneralLogFile {
        public ReloadGeneralLogFile() {
        }

        public static void execute(ManagerService service, String filePath) {
            try {
                LOCK.writeLock().lock();
                GeneralProvider.updateGeneralLogFile();
                filePath = StringUtil.removeAllApostrophe(filePath.trim()).trim();
                if (!filePath.startsWith(String.valueOf(File.separatorChar))) {
                    filePath = SystemConfig.getInstance().getHomePath() + File.separatorChar + filePath;
                }
                File newFilePath = new File(filePath);
                filePath = newFilePath.getCanonicalPath();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("start create general log file {}", filePath);
                }
                if (!isSuccessCreateFile(new File(filePath))) {
                    service.writeErrMessage(ErrorCode.ER_YES, "please check the permissions for the file path.");
                    return;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("end create general log file");
                }
                if (!filePath.equals(GeneralLog.getInstance().getGeneralLogFile())) {
                    WriteDynamicBootstrap.getInstance().changeValue("generalLogFile", filePath);
                    GeneralLog.getInstance().setGeneralLogFile(filePath);
                }
                GeneralLogHelper.putGLog(FILE_HEADER);
                OkPacket ok = new OkPacket();
                ok.setPacketId(1);
                ok.setAffectedRows(1);
                ok.setServerStatus(2);
                ok.setMessage(("reload general log path success").getBytes());
                ok.write(service.getConnection());
            } catch (Exception e) {
                String msg = "reload general log path failed";
                LOGGER.warn(service + " " + msg + " exception：" + e);
                service.writeErrMessage(ErrorCode.ER_YES, msg);
                return;
            } finally {
                LOCK.writeLock().unlock();
            }

        }

        private static boolean isSuccessCreateFile(File file) {
            try {
                if (!file.exists()) {
                    FileUtils.makeParentDirs(file);
                    file.createNewFile();
                }
                if (!file.canWrite()) {
                    return false;
                }
            } catch (IOException e) {
                LOGGER.warn("create general log file failed, exception：{}", e);
                return false;
            }
            return true;
        }
    }

}
