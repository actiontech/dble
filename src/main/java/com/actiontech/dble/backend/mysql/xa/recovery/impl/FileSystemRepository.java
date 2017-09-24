/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa.recovery.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.Deserializer;
import com.actiontech.dble.backend.mysql.xa.Serializer;
import com.actiontech.dble.backend.mysql.xa.VersionedFile;
import com.actiontech.dble.backend.mysql.xa.recovery.DeserialisationException;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.config.ServerConfig;
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
        ServerConfig config = DbleServer.getInstance().getConfig();
        SystemConfig systemConfig = config.getSystem();

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
            LOGGER.error(e.getMessage(), e);
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
        rwChannel.force(force);
    }

    @Override
    public CoordinatorLogEntry get(String coordinatorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        FileInputStream fis = null;
        try {
            fis = file.openLastValidVersionForReading();
        } catch (FileNotFoundException firstStart) {
            // the file could not be opened for reading;
            // merely return the default empty vector
            LOGGER.warn("FileSystemRepository.getAllCoordinatorLogEntries error", firstStart);
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
            LOGGER.error("Error in recover", e);
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
            LOGGER.warn(
                    "Unexpected EOF - logfile not closed properly last time?",
                    unexpectedEOF);
            // merely return what was read so far...
        } catch (DeserialisationException unexpectedEOF) {
            LOGGER.warn("Unexpected EOF - logfile not closed properly last time? " + unexpectedEOF);
        }
        return coordinatorLogEntries;
    }

    private static void closeSilently(BufferedReader fis) {
        try {
            if (fis != null)
                fis.close();
        } catch (IOException io) {
            LOGGER.warn("Fail to close logfile after reading - ignoring");
        }
    }

    private static CoordinatorLogEntry deserialize(String line)
            throws DeserialisationException {
        return Deserializer.fromJson(line);
    }

    @Override
    public void close() {
        try {
            closeOutput();
        } catch (Exception e) {
            LOGGER.warn("Error closing file - ignoring", e);
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
            return true;
        } catch (Exception e) {
            LOGGER.warn("Failed to write checkpoint", e);
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
