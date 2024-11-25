/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.status;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public final class LoadDataBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadDataBatch.class);
    private volatile int size;
    private volatile boolean enableBatchLoadData;
    private Set<String> successFileNames = ConcurrentHashMap.newKeySet();
    private int currentNodeSize = 0;
    private Map<String, List<String>> warnings = new HashMap<>();


    private LoadDataBatch() {
        size = SystemConfig.getInstance().getMaxRowSizeToFile();
        enableBatchLoadData = SystemConfig.getInstance().getEnableBatchLoadData() == 1 ? true : false;
    }

    private enum Singleton {
        INSTANCE;
        private LoadDataBatch instance;

        Singleton() {
            instance = new LoadDataBatch();
        }

        private LoadDataBatch getInstance() {
            return instance;
        }
    }


    public static LoadDataBatch getInstance() {
        return Singleton.INSTANCE.getInstance();
    }

    public boolean isEnableBatchLoadData() {
        return enableBatchLoadData;
    }

    public void setEnableBatchLoadData(boolean enableBatchLoadData) {
        this.enableBatchLoadData = enableBatchLoadData;
    }

    public int getSize() {
        return size;
    }

    public Set<String> getSuccessFileNames() {
        return successFileNames;
    }

    public void setSuccessFileNames(Set<String> successFileNames) {
        this.successFileNames = successFileNames;
    }

    public void setFileName(String fileName) {
        successFileNames.add(fileName);
    }

    public int getCurrentNodeSize() {
        return currentNodeSize;
    }

    public void setCurrentNodeSize(int currentNodeSize) {
        this.currentNodeSize = currentNodeSize;
    }

    public Map<String, List<String>> getWarnings() {
        return warnings;
    }

    public void setWarnings(Map<String, List<String>> warnings) {
        this.warnings = warnings;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void clean() {
        warnings.clear();
    }

    public void cleanAll() {
        clean();
        successFileNames.clear();
        currentNodeSize = 0;
    }

}
