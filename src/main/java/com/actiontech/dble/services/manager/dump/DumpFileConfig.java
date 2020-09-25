package com.actiontech.dble.services.manager.dump;

import java.io.File;

public class DumpFileConfig {

    private String readFile;
    private String defaultSchema;
    private int readQueueSize = 500;
    private String writePath;
    private int writeQueueSize = 500;
    private int maxValues = 4000;

    private boolean isIgnore = false;

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public String getReadFile() {
        return readFile;
    }

    public void setReadFile(String readFile) {
        this.readFile = readFile;
    }

    public int getReadQueueSize() {
        return readQueueSize;
    }

    public void setReadQueueSize(int readQueueSize) {
        this.readQueueSize = readQueueSize;
    }

    public String getWritePath() {
        return writePath;
    }

    public void setWritePath(String writePath) {
        if (!writePath.endsWith(File.separator)) {
            writePath += File.separator;
        }
        this.writePath = writePath;
    }

    public int getWriteQueueSize() {
        return writeQueueSize;
    }

    public void setWriteQueueSize(int writeQueueSize) {
        this.writeQueueSize = writeQueueSize;
    }

    public int getMaxValues() {
        return maxValues;
    }

    public void setMaxValues(int maxValues) {
        this.maxValues = maxValues;
    }

    public boolean isIgnore() {
        return isIgnore;
    }

    public void setIgnore(boolean ignore) {
        isIgnore = ignore;
    }
}
