/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa.recovery.impl;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.Deserializer;
import com.actiontech.dble.backend.mysql.xa.Serializer;
import com.actiontech.dble.backend.mysql.xa.VersionedFile;
import com.actiontech.dble.backend.mysql.xa.recovery.DeserializationException;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.actiontech.dble.backend.mysql.xa.XAStateLog.XA_ALERT_FLAG;

/**
 * Created by zhangchao on 2016/10/13.
 */
public class FileSystemRepository implements Repository {
    public static final Logger LOGGER = LoggerFactory.getLogger(FileSystemRepository.class);
    private VersionedFile file;
    private FileChannel rwChannel = null;

    public FileSystemRepository() {
        init();
    }

    @Override
    public void init() {
        SystemConfig systemConfig = SystemConfig.getInstance();

        String baseDir = systemConfig.getXaRecoveryLogBaseDir();
        String baseName = systemConfig.getXaRecoveryLogBaseName();

        LOGGER.debug("baseDir " + baseDir);
        LOGGER.debug("baseName " + baseName);

        //Judge whether exist the basedir
        createBaseDir(baseDir);

        file = new VersionedFile(baseDir, baseName, ".log");

    }

    @Override
    public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {

        try {
            initChannelIfNecessary();
            write(coordinatorLogEntry, true);
        } catch (IOException e) {
            AlertUtil.alertSelf(AlarmCode.XA_WRITE_IO_FAIL, Alert.AlertLevel.WARN, "Failed to writeDirectly logfile", null);
            LOGGER.warn(e.getMessage(), e);
        }
    }

    private synchronized void initChannelIfNecessary()
            throws FileNotFoundException {
        if (rwChannel == null) {
            rwChannel = file.openNewVersionForNioWriting();
        }
    }

    private void write(CoordinatorLogEntry coordinatorLogEntry,
                       boolean flushImmediately) throws IOException {
        String str = Serializer.toJson(coordinatorLogEntry);
        byte[] buffer = str.getBytes();
        ByteBuffer buff = ByteBuffer.wrap(buffer);
        writeToFile(buff, flushImmediately);
    }

    private synchronized void writeToFile(ByteBuffer buff, boolean force)
            throws IOException {
        rwChannel.write(buff);
    }

    @Override
    public CoordinatorLogEntry get(String coordinatorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries(boolean first) {
        FileInputStream fis = null;
        try {
            fis = file.openLastValidVersionForReading();
        } catch (FileNotFoundException firstStart) {
            // the file could not be opened for reading;
            // merely return the default empty vector
            if (!first) {
                LOGGER.info("Only For debug FileSystemRepository.getAllCoordinatorLogEntries error", firstStart);
            }
        }
        if (fis != null) {
            return readFromInputStream(fis);
        }
        //else
        return Collections.emptyList();
    }

    private static Collection<CoordinatorLogEntry> readFromInputStream(
            InputStream in) {
        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
        BufferedReader br = null;
        try {
            InputStreamReader isr = new InputStreamReader(in);
            br = new BufferedReader(isr);
            coordinatorLogEntries = readContent(br);
        } catch (Exception e) {
            LOGGER.warn("Error in recover", e);
            AlertUtil.alertSelf(AlarmCode.XA_READ_IO_FAIL, Alert.AlertLevel.WARN, "Error in recover:" + e.getMessage(), null);

        } finally {
            closeSilently(br);
        }
        return coordinatorLogEntries.values();
    }

    private static Map<String, CoordinatorLogEntry> readContent(BufferedReader br)
            throws IOException {

        Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                CoordinatorLogEntry coordinatorLogEntry = deserialize(line);
                coordinatorLogEntries.put(coordinatorLogEntry.getId(),
                        coordinatorLogEntry);
            }

        } catch (EOFException unexpectedEOF) {
            LOGGER.info(
                    "Unexpected EOF - logfile not closed properly last time?",
                    unexpectedEOF);
            // merely return what was read so far...
        } catch (ObjectStreamException unexpectedEOF) {
            LOGGER.warn("Unexpected EOF - logfile not closed properly last time?", unexpectedEOF);
            AlertUtil.alertSelf(AlarmCode.XA_READ_XA_STREAM_FAIL, Alert.AlertLevel.WARN,
                    "Unexpected EOF - logfile not closed properly last time?" + unexpectedEOF.getMessage(), null);
            // merely return what was read so far...
        } catch (DeserializationException unexpectedEOF) {
            LOGGER.warn("DeserializationException - logfile not closed properly last time? ", unexpectedEOF);
            AlertUtil.alertSelf(AlarmCode.XA_READ_DECODE_FAIL, Alert.AlertLevel.WARN,
                    "DeserializationException - logfile not closed properly last time? " + unexpectedEOF.getMessage(), null);
        }
        return coordinatorLogEntries;
    }

    private static void closeSilently(BufferedReader fis) {
        try {
            if (fis != null)
                fis.close();
        } catch (IOException io) {
            LOGGER.info("Fail to close logfile after reading - ignoring");
        }
    }

    private static CoordinatorLogEntry deserialize(String line)
            throws DeserializationException {
        return Deserializer.fromJson(line);
    }

    @Override
    public void close() {
        try {
            closeOutput();
        } catch (Exception e) {
            LOGGER.info("Error closing file - ignoring", e);
        }

    }

    private void closeOutput() throws IllegalStateException {
        try {
            if (file != null) {
                file.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error closing previous output", e);
        }
    }

    @Override
    public boolean writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) {
        try {
            closeOutput();
            file.rotateFileVersion();
            rwChannel = file.openNewVersionForNioWriting();
            for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
                write(coordinatorLogEntry, false);
            }
            rwChannel.force(false);
            file.discardBackupVersion();
            if (ToResolveContainer.XA_WRITE_CHECK_POINT_FAIL.size() > 0) {
                AlertUtil.alertSelfResolve(AlarmCode.XA_WRITE_CHECK_POINT_FAIL, Alert.AlertLevel.WARN, null, ToResolveContainer.XA_WRITE_CHECK_POINT_FAIL, XA_ALERT_FLAG);
            }
            return true;
        } catch (Exception e) {
            LOGGER.warn("Failed to writeDirectly checkpoint", e);
            AlertUtil.alertSelf(AlarmCode.XA_WRITE_CHECK_POINT_FAIL, Alert.AlertLevel.WARN, "Failed to writeDirectly checkpoint:" + e.getMessage(), null);
            ToResolveContainer.XA_WRITE_CHECK_POINT_FAIL.add(XA_ALERT_FLAG);
            return false;
        }
    }

    /**
     * create the log base dir
     */
    private void createBaseDir(String baseDir) {
        File baseDirFolder = new File(baseDir);
        if (!baseDirFolder.exists()) {
            baseDirFolder.mkdirs();
        }
    }

    @Override
    public void remove(String id) {
        throw new UnsupportedOperationException();
    }

}
