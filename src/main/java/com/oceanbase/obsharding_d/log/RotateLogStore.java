/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log;

import com.oceanbase.obsharding_d.log.general.LogEntry;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class RotateLogStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotateLogStore.class);
    private RotateStrategy rotateStrategy;

    private static final RotateLogStore INSTANCE = new RotateLogStore();

    public static RotateLogStore getInstance() {
        return INSTANCE;
    }

    public RotateLogStore() {
        rotateStrategy = new RotateStrategy();
    }

    public LogFileManager createFileManager(String name, int maxFileSize) throws IOException {
        File file;
        RandomAccessFile raf = null;
        long currentSize = 0;
        long initialTime = System.currentTimeMillis();
        boolean initFileExists = false;
        try {
            file = new File(name);
            if (file.exists()) {
                currentSize = file.length();
                initialTime = file.lastModified();
                initFileExists = true;
            }
            FileUtils.makeParentDirs(file);
            raf = new RandomAccessFile(name, "rw");
            raf.seek(raf.length());
        } catch (IOException ex) {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    LOGGER.warn("Create file failure and close exception：{}", e);
                }
            }
            throw new IOException(ex);
        }
        return new LogFileManager(name, maxFileSize, raf, currentSize, initialTime, initFileExists);
    }

    public class LogFileManager {
        private volatile String fileName;
        private long maxFileSize;
        private volatile RandomAccessFile randomAccessFile;
        private long size;
        private long initialTime;
        private long nextRolloverMillis = 0;
        private ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[4096]); // tentative
        private Calendar cal;
        // private boolean initFileExists = false;

        public LogFileManager(String fileName, int maxFileSize, RandomAccessFile raf, long size, long initialTime, boolean initFileExists) {
            this.fileName = fileName;
            this.maxFileSize = 1024L * 1024 * maxFileSize;
            this.randomAccessFile = raf;
            this.size = size;
            this.initialTime = initialTime;
            this.cal = Calendar.getInstance();
            // this.initFileExists = initFileExists;
        }

        public void init() {
            isTriggeringEvent(true, null);
        }

        public void append(LogEntry logEntry, boolean isEndOfBatch) throws IOException {
            checkRollover(logEntry);
            writeData(logEntry.toLog(), isEndOfBatch);
        }

        public void close() {
            closeFileStream();
        }

        public void reset() {
            closeFileStream();
            try {
                size = 0;
                File file = new File(fileName);
                /*if (file.exists()) {
                    file.delete();
                }*/
                FileUtils.makeParentDirs(file);
                reCreateFile();
            } catch (IOException e) {
                LOGGER.warn("Failed to reset file, exception：{}", e);
            }
        }

        public long getFileSize() {
            return size + byteBuffer.position();
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        private void writeData(String data, boolean isEndOfBatch) throws IOException {
            ByteBuffer tmpBuffer = ByteBuffer.wrap(data.getBytes("utf-8"));
            if (tmpBuffer.limit() > byteBuffer.remaining()) {
                flushBuffer();
                randomAccessFile.write(tmpBuffer.array(), 0, tmpBuffer.limit());
                size += tmpBuffer.limit();
            } else {
                byteBuffer.put(tmpBuffer);
                if (isEndOfBatch) {
                    flushBuffer();
                }
            }
        }

        private synchronized void checkRollover(LogEntry logEntry) {
            if (isTriggeringEvent(false, logEntry)) {
                rollover();
            }
        }

        private boolean isTriggeringEvent(boolean isInit, LogEntry logEntry) {
            if (isInit) {
                long currentTime = System.currentTimeMillis();
                nextRolloverMillis = getNextTime(currentTime);
                if (getFileSize() > 0 && !DateUtils.isSameDay(new Date(initialTime), new Date(currentTime))) {
                    rollover();
                }
                return false;
                // startup does not trigger rollover
                /*if (initFileExists) {
                    rollover();
                    return false;
                }*/
            }

            // size
            if (getFileSize() >= maxFileSize) {
                return true;
            }

            // daily
            if (logEntry != null && logEntry.getTime() > nextRolloverMillis) {
                nextRolloverMillis = getNextTime(logEntry.getTime());
                if (getFileSize() > 0) {
                    return true;
                }
            }
            return false;
        }

        private synchronized void rollover() {
            if (rotateStrategy.rollover(this)) {
                try {
                    this.size = 0;
                    reCreateFile();
                } catch (IOException e) {
                    LOGGER.warn("Failed to create file after rollover, exception：{}", e);
                }
            }
        }

        private void reCreateFile() throws IOException {
            this.randomAccessFile = new RandomAccessFile(fileName, "rw");
            randomAccessFile.seek(randomAccessFile.length());
        }

        private synchronized void flushBuffer() {
            byteBuffer.flip();
            int remaining = byteBuffer.remaining();
            if (remaining > 0) {
                try {
                    randomAccessFile.write(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), remaining);
                    size += remaining;
                } catch (IOException ex) {
                    LOGGER.warn("Buffer data write to disk exception：{}", ex);
                }
            }
            byteBuffer.clear();
        }

        private synchronized boolean closeFileStream() {
            flushBuffer();
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                    return true;
                } catch (final IOException e) {
                    LOGGER.warn("Close file stream exception：{}", e);
                    return false;
                }
            }
            return true;
        }

        private long getNextTime(long currentMillis) {
            cal.setTimeInMillis(currentMillis);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            cal.add(Calendar.DATE, 1);
            return cal.getTimeInMillis();
        }
    }

    public static class RotateStrategy {
        private boolean rollover(LogFileManager manager) {
            manager.closeFileStream();
            File currentFile = new File(manager.getFileName());
            int suffixIndex = currentFile.getName().lastIndexOf(".");
            String suffix = "";
            String currentFileName = currentFile.getName();
            if (suffixIndex != -1) {
                suffix = currentFileName.substring(suffixIndex);
                currentFileName = currentFileName.substring(0, suffixIndex);
            }
            String[] renameInfo = getRenameInfo(currentFile.getParent(), currentFileName, suffix, currentFile.lastModified());
            Integer fileIndex = FileUtils.getFileNextIndex(renameInfo[0], renameInfo[1], suffix);
            String renameTo = String.format(renameInfo[1], fileIndex);
            return rolloverExecute(currentFile, new File(renameTo));
        }

        private String[] getRenameInfo(String parentPath, String name, String suffix, long time) {
            String[] str = new SimpleDateFormat("yyyy-MM/MM-dd").format(new Date(time)).split("/");
            StringBuffer filePatternSbStr = new StringBuffer();
            filePatternSbStr.append(name);
            filePatternSbStr.append("-");
            filePatternSbStr.append(str[1]);
            filePatternSbStr.append("-");

            StringBuffer sbStr = new StringBuffer();
            sbStr.append(parentPath);
            sbStr.append(File.separatorChar);
            sbStr.append(str[0]);
            sbStr.append(File.separatorChar);
            sbStr.append(filePatternSbStr.toString());
            sbStr.append("%d");
            sbStr.append(suffix);

            String[] arr = new String[2];
            arr[0] = filePatternSbStr.toString();
            arr[1] = sbStr.toString();

            return arr;
        }

        private boolean rolloverExecute(File source, File destination) {
            // move || rename || copy
            try {
                return moveFile(Paths.get(source.getAbsolutePath()), Paths.get(destination.getAbsolutePath()));
            } catch (IOException exMove) {
                boolean result = source.renameTo(destination);
                if (!result) {
                    try {
                        Files.copy(Paths.get(source.getAbsolutePath()), Paths.get(destination.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
                        try {
                            Files.delete(Paths.get(source.getAbsolutePath()));
                            LOGGER.warn("Renamed file {} to {} using copy and delete",
                                    source.getAbsolutePath(), destination.getAbsolutePath());
                        } catch (IOException exDelete) {
                            LOGGER.error("Unable to delete file {}: {} {}", source.getAbsolutePath(),
                                    exDelete.getClass().getName(), exDelete);
                            new PrintWriter(source.getAbsolutePath()).close();
                        }
                    } catch (IOException exCopy) {
                        LOGGER.warn("Copy file {} to {} with source.renameTo",
                                source.getAbsolutePath(), destination.getAbsolutePath(), exCopy);
                    }
                } else {
                    LOGGER.warn("Renamed file {} to {} with source.renameTo",
                            source.getAbsolutePath(), destination.getAbsolutePath());
                }
                return true;
            }
        }

        private boolean moveFile(Path original, Path target) throws IOException {
            try {
                Files.move(original, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.warn("Renamed file {} to {} with Files.move", original.toFile().getAbsolutePath(),
                        target.toFile().getAbsolutePath());
                return true;
            } catch (final AtomicMoveNotSupportedException ex) {
                Files.move(original, target, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.warn("Renamed file {} to {} with Files.move", original.toFile().getAbsolutePath(),
                        target.toFile().getAbsolutePath());
                return true;
            }
        }
    }
}
