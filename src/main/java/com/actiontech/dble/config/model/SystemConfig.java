/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.memory.unsafe.Platform;

import java.io.File;
import java.io.IOException;


/**
 * SystemConfig
 *
 * @author mycat
 */
public final class SystemConfig {
    public static final String SYS_HOME = "DBLE_HOME";
    static final long DEFAULT_IDLE_TIMEOUT = 30 * 60 * 1000L;
    public static final int SEQUENCE_HANDLER_MYSQL = 1;
    public static final int SEQUENCE_HANDLER_LOCAL_TIME = 2;
    public static final int SEQUENCE_HANDLER_ZK_DISTRIBUTED = 3;
    public static final int SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT = 4;
    private static final String WARNING_FORMATE = "Property [ %s ] '%d' in server.xml is illegal, use %d replaced";
    /*
     * the supported  protocol version of MySQL
     * For Other MySQL branch ,like MariaDB 10.1.x,
     * but its protocol is compatible with MySQL. So the versions array only contain official version here
     */
    public static final String[] MYSQL_VERSIONS = {"5.5", "5.6", "5.7"};

    private static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();

    // base config
    private String bindIp = "0.0.0.0";
    private int serverPort = 8066;
    private int managerPort = 9066;
    private int processors = DEFAULT_PROCESSORS;
    private int backendProcessors = DEFAULT_PROCESSORS;
    private int processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS : 2;
    private int backendProcessorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS : 2;
    private int complexExecutor = processorExecutor > 8 ? 8 : processorExecutor;
    private int writeToBackendExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS : 2;
    private String fakeMySQLVersion = null;
    private int sequnceHandlerType = SEQUENCE_HANDLER_LOCAL_TIME;
    private int serverBacklog = 2048;
    private int serverNodeId = 1;
    private long showBinlogStatusTimeout = 60 * 1000;
    private int maxCon = 0;
    //option
    private int useCompression = 0;
    private int usingAIO = 0;
    private int useThreadUsageStat = 0;
    private int usePerformanceMode = 0;
    private int useSerializableMode = 0;

    //query time cost statistics
    private int useCostTimeStat = 0;
    private int maxCostStatSize = 100;
    private int costSamplePercent = 1;
    //connection
    private String charset = "utf8mb4";
    private int maxPacketSize = 4 * 1024 * 1024;
    private int txIsolation = Isolations.REPEATABLE_READ;
    private int autocommit = 1;

    //consistency
    private int checkTableConsistency = 0;
    private long checkTableConsistencyPeriod = 30 * 60 * 1000;

    //heartbeat check period
    private long dataNodeIdleCheckPeriod = 5 * 60 * 1000L;
    private long dataNodeHeartbeatPeriod = 10 * 1000L;

    //processor check conn
    private long processorCheckPeriod = 1000L;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    // sql execute timeout (second)
    private long sqlExecuteTimeout = 300;

    //transaction log
    private int recordTxn = 0;
    private String transactionLogBaseDir = SystemConfig.getHomePath() + File.separatorChar + "txlogs" + File.separatorChar;
    private String transactionLogBaseName = "server-tx";
    private int transactionRatateSize = 16; // M

    //XA transaction
    private long xaSessionCheckPeriod = 1000L;
    private long xaLogCleanPeriod = 1000L;
    private String xaRecoveryLogBaseDir = SystemConfig.getHomePath() + File.separatorChar + "tmlogs" + File.separatorChar;
    private String xaRecoveryLogBaseName = "tmlog";
    private int xaRetryCount = 0;

    //use JoinStrategy
    private boolean useJoinStrategy = false;
    private int nestLoopRowsSize = 2000;
    private int nestLoopConnSize = 4;

    //query memory used for per session,unit is M
    private int otherMemSize = 4;
    private int orderMemSize = 4;
    private int joinMemSize = 4;

    // off Heap unit:bytes
    // a page size
    private int bufferPoolPageSize = 1024 * 1024 * 2;
    //minimum allocation unit
    private short bufferPoolChunkSize = 4096;
    // buffer pool page number
    private short bufferPoolPageNumber = (short) (Platform.getMaxDirectMemory() * 0.8 / bufferPoolPageSize);
    private boolean useDefaultPageNumber = true;
    private int mappedFileSize = 1024 * 1024 * 64;

    // sql statistics
    private int useSqlStat = 1;
    private int sqlRecordCount = 10;
    //Threshold of big result ,default512kb
    private int maxResultSet = 512 * 1024;
    //Threshold of Usage Percent of buffer pool,if reached the Threshold,big result will be clean up,default 80%
    private int bufferUsagePercent = 80;
    //period of clear the big result
    private long clearBigSqLResultSetMapMs = 10 * 60 * 1000;

    //frontSocket unit:bytes
    private int frontSocketSoRcvbuf = 1024 * 1024;
    private int frontSocketSoSndbuf = 4 * 1024 * 1024;
    private int frontSocketNoDelay = 1; // 0=false

    // backSocket unit:bytes
    private int backSocketSoRcvbuf = 4 * 1024 * 1024;
    private int backSocketSoSndbuf = 1024 * 1024;
    private int backSocketNoDelay = 1; // 1=true

    //view
    private String viewPersistenceConfBaseDir = SystemConfig.getHomePath() + File.separatorChar + "viewConf" + File.separatorChar;
    private String viewPersistenceConfBaseName = "viewJson";

    // for join tmp results
    private int mergeQueueSize = 1024;
    private int orderByQueueSize = 1024;
    private int joinQueueSize = 1024;
    //slow log
    private int enableSlowLog = 0;
    private String slowLogBaseDir = SystemConfig.getHomePath() + File.separatorChar + "slowlogs" + File.separatorChar;
    private String slowLogBaseName = "slow-query";
    private int flushSlowLogPeriod = 1; //second
    private int flushSlowLogSize = 1000; //row
    private int sqlSlowTime = 100; //ms
    //alert switch
    private int enableAlert = 1;
    //load data
    private int maxRowSizeToFile = 10000;
    private int maxCharsPerColumn = 65535; // 128k,65535 chars
    //errors
    private ProblemReporter problemReporter;
    private boolean useOuterHa = false;

    public SystemConfig(ProblemReporter problemReporter) {
        this.problemReporter = problemReporter;
    }

    public int getTransactionRatateSize() {
        return transactionRatateSize;
    }

    @SuppressWarnings("unused")
    public void setTransactionRatateSize(int transactionRatateSize) {
        if (transactionRatateSize > 0) {
            this.transactionRatateSize = transactionRatateSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "transactionRatateSize", transactionRatateSize, this.transactionRatateSize));
        }
    }

    public String getTransactionLogBaseDir() {
        return transactionLogBaseDir;
    }

    @SuppressWarnings("unused")
    public void setTransactionLogBaseDir(String transactionLogBaseDir) {
        this.transactionLogBaseDir = transactionLogBaseDir;
    }

    public String getTransactionLogBaseName() {
        return transactionLogBaseName;
    }

    @SuppressWarnings("unused")
    public void setTransactionLogBaseName(String transactionLogBaseName) {
        this.transactionLogBaseName = transactionLogBaseName;
    }

    public boolean isUseJoinStrategy() {
        return useJoinStrategy;
    }

    @SuppressWarnings("unused")
    public void setUseJoinStrategy(boolean useJoinStrategy) {
        this.useJoinStrategy = useJoinStrategy;
    }

    public String getXaRecoveryLogBaseDir() {
        return xaRecoveryLogBaseDir;
    }

    @SuppressWarnings("unused")
    public void setXaRecoveryLogBaseDir(String xaRecoveryLogBaseDir) {
        this.xaRecoveryLogBaseDir = xaRecoveryLogBaseDir;
    }

    public String getXaRecoveryLogBaseName() {
        return xaRecoveryLogBaseName;
    }

    @SuppressWarnings("unused")
    public void setXaRecoveryLogBaseName(String xaRecoveryLogBaseName) {
        this.xaRecoveryLogBaseName = xaRecoveryLogBaseName;
    }


    public int getSequnceHandlerType() {
        return sequnceHandlerType;
    }

    @SuppressWarnings("unused")
    public void setSequnceHandlerType(int sequnceHandlerType) {
        if (sequnceHandlerType >= 1 && sequnceHandlerType <= 4) {
            this.sequnceHandlerType = sequnceHandlerType;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "sequnceHandlerType", sequnceHandlerType, this.sequnceHandlerType));
        }
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    @SuppressWarnings("unused")
    public void setMaxPacketSize(int maxPacketSize) {
        if (maxPacketSize >= 1024 && maxPacketSize <= 1073741824) {
            this.maxPacketSize = maxPacketSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxPacketSize", maxPacketSize, this.maxPacketSize));
        }
    }

    public String getBindIp() {
        return bindIp;
    }

    @SuppressWarnings("unused")
    public void setBindIp(String bindIp) {
        this.bindIp = bindIp;
    }

    public static String getHomePath() {
        String home = System.getProperty(SystemConfig.SYS_HOME);
        if (home != null && home.endsWith(File.pathSeparator)) {
            home = home.substring(0, home.length() - 1);
            System.setProperty(SystemConfig.SYS_HOME, home);
        }

        // if HOMEis not set,set it as current path or parent path
        if (home == null) {
            try {
                String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
                File conf = new File(path + "/conf");
                if (conf.exists() && conf.isDirectory()) {
                    home = path;
                } else {
                    path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
                    conf = new File(path + "/conf");
                    if (conf.exists() && conf.isDirectory()) {
                        home = path;
                    }
                }

                if (home != null) {
                    System.setProperty(SystemConfig.SYS_HOME, home);
                }
            } catch (IOException e) {
                //ignore error
            }
        }

        return home;
    }

    public int getUseSqlStat() {
        return useSqlStat;
    }

    @SuppressWarnings("unused")
    public void setUseSqlStat(int useSqlStat) {
        if (useSqlStat >= 0 && useSqlStat <= 1) {
            this.useSqlStat = useSqlStat;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "useSqlStat", useSqlStat, this.useSqlStat));
        }
    }

    public int getUseCompression() {
        return useCompression;
    }

    @SuppressWarnings("unused")
    public void setUseCompression(int useCompression) {
        if (useCompression >= 0 && useCompression <= 1) {
            this.useCompression = useCompression;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "useCompression", useCompression, this.useCompression));
        }
    }

    public String getCharset() {
        return charset;
    }

    @SuppressWarnings("unused")
    public void setCharset(String charset) {
        if (CharsetUtil.getCharsetDefaultIndex(charset) > 0) {
            this.charset = charset;
        } else if (this.problemReporter != null) {
            problemReporter.warn("Property [ charset ] '" + charset + "' in server.xml is illegal, use " + this.charset + " replaced");
        }
    }

    public String getFakeMySQLVersion() {
        return fakeMySQLVersion;
    }

    @SuppressWarnings("unused")
    public void setFakeMySQLVersion(String mysqlVersion) {
        this.fakeMySQLVersion = mysqlVersion;
    }

    public int getServerPort() {
        return serverPort;
    }

    @SuppressWarnings("unused")
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getServerBacklog() {
        return serverBacklog;
    }

    @SuppressWarnings("unused")
    public void setServerBacklog(int serverBacklog) {
        this.serverBacklog = serverBacklog;
    }

    public int getManagerPort() {
        return managerPort;
    }

    @SuppressWarnings("unused")
    public void setManagerPort(int managerPort) {
        this.managerPort = managerPort;
    }

    public int getProcessors() {
        return processors;
    }

    @SuppressWarnings("unused")
    public void setProcessors(int processors) {
        if (processors > 0) {
            this.processors = processors;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "processors", processors, this.processors));
        }
    }

    public int getBackendProcessors() {
        return backendProcessors;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessors(int backendProcessors) {
        if (backendProcessors > 0) {
            this.backendProcessors = backendProcessors;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "backendProcessors", backendProcessors, this.backendProcessors));
        }
    }

    public int getProcessorExecutor() {
        return processorExecutor;
    }

    @SuppressWarnings("unused")
    public void setProcessorExecutor(int processorExecutor) {
        if (processorExecutor > 0) {
            this.processorExecutor = processorExecutor;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "processorExecutor", processorExecutor, this.processorExecutor));
        }
    }

    public int getBackendProcessorExecutor() {
        return backendProcessorExecutor;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessorExecutor(int backendProcessorExecutor) {
        if (backendProcessorExecutor > 0) {
            this.backendProcessorExecutor = backendProcessorExecutor;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "backendProcessorExecutor", backendProcessorExecutor, this.backendProcessorExecutor));
        }
    }

    public int getComplexExecutor() {
        return complexExecutor;
    }

    @SuppressWarnings("unused")
    public void setComplexExecutor(int complexExecutor) {
        if (complexExecutor > 0) {
            this.complexExecutor = complexExecutor;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "complexExecutor", complexExecutor, this.complexExecutor));
        }
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    @SuppressWarnings("unused")
    public void setIdleTimeout(long idleTimeout) {
        if (idleTimeout > 0) {
            this.idleTimeout = idleTimeout;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "idleTimeout", idleTimeout, this.idleTimeout));
        }
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        if (processorCheckPeriod > 0) {
            this.processorCheckPeriod = processorCheckPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "processorCheckPeriod", processorCheckPeriod, this.processorCheckPeriod));
        }
    }

    public long getXaSessionCheckPeriod() {
        return xaSessionCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaSessionCheckPeriod(long xaSessionCheckPeriod) {
        if (xaSessionCheckPeriod > 0) {
            this.xaSessionCheckPeriod = xaSessionCheckPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "xaSessionCheckPeriod", xaSessionCheckPeriod, this.xaSessionCheckPeriod));
        }
    }

    public long getXaLogCleanPeriod() {
        return xaLogCleanPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaLogCleanPeriod(long xaLogCleanPeriod) {
        if (xaLogCleanPeriod > 0) {
            this.xaLogCleanPeriod = xaLogCleanPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "xaLogCleanPeriod", xaLogCleanPeriod, this.xaLogCleanPeriod));
        }
    }

    public long getDataNodeIdleCheckPeriod() {
        return dataNodeIdleCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setDataNodeIdleCheckPeriod(long dataNodeIdleCheckPeriod) {
        if (dataNodeIdleCheckPeriod > 0) {
            this.dataNodeIdleCheckPeriod = dataNodeIdleCheckPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "dataNodeIdleCheckPeriod", dataNodeIdleCheckPeriod, this.dataNodeIdleCheckPeriod));
        }
    }

    public long getDataNodeHeartbeatPeriod() {
        return dataNodeHeartbeatPeriod;
    }

    @SuppressWarnings("unused")
    public void setDataNodeHeartbeatPeriod(long dataNodeHeartbeatPeriod) {
        if (dataNodeHeartbeatPeriod > 0) {
            this.dataNodeHeartbeatPeriod = dataNodeHeartbeatPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "dataNodeHeartbeatPeriod", dataNodeHeartbeatPeriod, this.dataNodeHeartbeatPeriod));
        }
    }

    public long getSqlExecuteTimeout() {
        return sqlExecuteTimeout;
    }

    @SuppressWarnings("unused")
    public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
        if (sqlExecuteTimeout > 0) {
            this.sqlExecuteTimeout = sqlExecuteTimeout;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "sqlExecuteTimeout", sqlExecuteTimeout, this.sqlExecuteTimeout));
        }
    }


    public long getShowBinlogStatusTimeout() {
        return showBinlogStatusTimeout;
    }

    @SuppressWarnings("unused")
    public void setShowBinlogStatusTimeout(long showBinlogStatusTimeout) {
        if (showBinlogStatusTimeout > 0) {
            this.showBinlogStatusTimeout = sqlExecuteTimeout;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "showBinlogStatusTimeout", showBinlogStatusTimeout, this.showBinlogStatusTimeout));
        }
    }


    public int getTxIsolation() {
        return txIsolation;
    }

    @SuppressWarnings("unused")
    public void setTxIsolation(int txIsolation) {
        if (txIsolation >= 1 && txIsolation <= 4) {
            this.txIsolation = txIsolation;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "txIsolation", txIsolation, this.txIsolation));
        }
    }

    public int getAutocommit() {
        return autocommit;
    }

    @SuppressWarnings("unused")
    public void setAutocommit(int autocommit) {
        if (autocommit >= 0 && autocommit <= 1) {
            this.autocommit = autocommit;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "autocommit", autocommit, this.autocommit));
        }
    }


    public int getSqlRecordCount() {
        return sqlRecordCount;
    }

    @SuppressWarnings("unused")
    public void setSqlRecordCount(int sqlRecordCount) {
        if (sqlRecordCount > 0) {
            this.sqlRecordCount = sqlRecordCount;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "sqlRecordCount", sqlRecordCount, this.sqlRecordCount));
        }
    }

    public int getRecordTxn() {
        return recordTxn;
    }

    @SuppressWarnings("unused")
    public void setRecordTxn(int recordTxn) {
        if (recordTxn >= 0 && recordTxn <= 1) {
            this.recordTxn = recordTxn;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "recordTxn", recordTxn, this.recordTxn));
        }
    }

    public short getBufferPoolChunkSize() {
        return bufferPoolChunkSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
        if (bufferPoolChunkSize > 0) {
            this.bufferPoolChunkSize = bufferPoolChunkSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "bufferPoolChunkSize", bufferPoolChunkSize, this.bufferPoolChunkSize));
        }
    }

    public int getMaxResultSet() {
        return maxResultSet;
    }

    @SuppressWarnings("unused")
    public void setMaxResultSet(int maxResultSet) {
        if (maxResultSet > 0) {
            this.maxResultSet = maxResultSet;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxResultSet", maxResultSet, this.maxResultSet));
        }
    }

    public int getBufferUsagePercent() {
        return bufferUsagePercent;
    }

    @SuppressWarnings("unused")
    public void setBufferUsagePercent(int bufferUsagePercent) {
        if (bufferUsagePercent >= 0 && bufferUsagePercent <= 100) {
            this.bufferUsagePercent = bufferUsagePercent;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "bufferUsagePercent", bufferUsagePercent, this.bufferUsagePercent));
        }
    }

    public long getClearBigSqLResultSetMapMs() {
        return clearBigSqLResultSetMapMs;
    }

    @SuppressWarnings("unused")
    public void setClearBigSqLResultSetMapMs(long clearBigSqLResultSetMapMs) {
        if (clearBigSqLResultSetMapMs > 0) {
            this.clearBigSqLResultSetMapMs = clearBigSqLResultSetMapMs;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "clearBigSqLResultSetMapMs", clearBigSqLResultSetMapMs, this.clearBigSqLResultSetMapMs));
        }
    }

    public int getBufferPoolPageSize() {
        return bufferPoolPageSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolPageSize(int bufferPoolPageSize) {
        if (bufferPoolPageSize > 0) {
            this.bufferPoolPageSize = bufferPoolPageSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "bufferPoolPageSize", bufferPoolPageSize, this.bufferPoolPageSize));
        }
    }

    public short getBufferPoolPageNumber() {
        return bufferPoolPageNumber;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolPageNumber(short bufferPoolPageNumber) {
        if (bufferPoolPageNumber > 0) {
            this.bufferPoolPageNumber = bufferPoolPageNumber;
            useDefaultPageNumber = false;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "bufferPoolPageNumber", bufferPoolPageNumber, this.bufferPoolPageNumber));
        }
    }



    public boolean isUseDefaultPageNumber() {
        return useDefaultPageNumber;
    }


    public int getFrontSocketSoRcvbuf() {
        return frontSocketSoRcvbuf;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
        if (frontSocketSoRcvbuf > 0) {
            this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "frontSocketSoRcvbuf", frontSocketSoRcvbuf, this.frontSocketSoRcvbuf));
        }
    }

    public int getFrontSocketSoSndbuf() {
        return frontSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
        if (frontSocketSoSndbuf > 0) {
            this.frontSocketSoSndbuf = frontSocketSoSndbuf;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "frontSocketSoSndbuf", frontSocketSoSndbuf, this.frontSocketSoSndbuf));
        }
    }

    public int getBackSocketSoRcvbuf() {
        return backSocketSoRcvbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
        if (backSocketSoRcvbuf > 0) {
            this.backSocketSoRcvbuf = backSocketSoRcvbuf;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "backSocketSoRcvbuf", backSocketSoRcvbuf, this.backSocketSoRcvbuf));
        }
    }

    public int getBackSocketSoSndbuf() {
        return backSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
        if (backSocketSoSndbuf > 0) {
            this.backSocketSoSndbuf = backSocketSoSndbuf;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "backSocketSoSndbuf", backSocketSoSndbuf, this.backSocketSoSndbuf));
        }
    }

    public int getFrontSocketNoDelay() {
        return frontSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketNoDelay(int frontSocketNoDelay) {
        if (frontSocketNoDelay >= 0 && frontSocketNoDelay <= 1) {
            this.frontSocketNoDelay = frontSocketNoDelay;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "frontSocketNoDelay", frontSocketNoDelay, this.frontSocketNoDelay));
        }
    }

    public int getBackSocketNoDelay() {
        return backSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setBackSocketNoDelay(int backSocketNoDelay) {
        if (backSocketNoDelay >= 0 && backSocketNoDelay <= 1) {
            this.backSocketNoDelay = backSocketNoDelay;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "backSocketNoDelay", backSocketNoDelay, this.backSocketNoDelay));
        }
    }

    public int getUsingAIO() {
        return usingAIO;
    }

    @SuppressWarnings("unused")
    public void setUsingAIO(int usingAIO) {
        if (usingAIO >= 0 && usingAIO <= 1) {
            this.usingAIO = usingAIO;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "usingAIO", usingAIO, this.usingAIO));
        }
    }

    public int getServerNodeId() {
        return serverNodeId;
    }

    @SuppressWarnings("unused")
    public void setServerNodeId(int serverNodeId) {
        if (serverNodeId > 0) {
            this.serverNodeId = serverNodeId;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "serverNodeId", serverNodeId, this.serverNodeId));
        }
    }

    public int getCheckTableConsistency() {
        return checkTableConsistency;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistency(int checkTableConsistency) {
        if (checkTableConsistency >= 0 && checkTableConsistency <= 1) {
            this.checkTableConsistency = checkTableConsistency;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "checkTableConsistency", checkTableConsistency, this.checkTableConsistency));
        }
    }

    public long getCheckTableConsistencyPeriod() {
        return checkTableConsistencyPeriod;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistencyPeriod(long checkTableConsistencyPeriod) {
        if (checkTableConsistencyPeriod > 0) {
            this.checkTableConsistencyPeriod = checkTableConsistencyPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "checkTableConsistencyPeriod", checkTableConsistencyPeriod, this.checkTableConsistencyPeriod));
        }
    }

    public int getNestLoopRowsSize() {
        return nestLoopRowsSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopRowsSize(int nestLoopRowsSize) {
        if (nestLoopRowsSize > 0) {
            this.nestLoopRowsSize = nestLoopRowsSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "nestLoopRowsSize", nestLoopRowsSize, this.nestLoopRowsSize));
        }
    }

    public int getJoinQueueSize() {
        return joinQueueSize;
    }

    @SuppressWarnings("unused")
    public void setJoinQueueSize(int joinQueueSize) {
        if (joinQueueSize > 0) {
            this.joinQueueSize = joinQueueSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "joinQueueSize", joinQueueSize, this.joinQueueSize));
        }
    }

    public int getMergeQueueSize() {
        return mergeQueueSize;
    }

    @SuppressWarnings("unused")
    public void setMergeQueueSize(int mergeQueueSize) {
        if (mergeQueueSize > 0) {
            this.mergeQueueSize = mergeQueueSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "mergeQueueSize", mergeQueueSize, this.mergeQueueSize));
        }
    }

    public int getOtherMemSize() {
        return otherMemSize;
    }

    @SuppressWarnings("unused")
    public void setOtherMemSize(int otherMemSize) {
        if (otherMemSize > 0) {
            this.otherMemSize = otherMemSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "otherMemSize", otherMemSize, this.otherMemSize));
        }
    }

    public int getOrderMemSize() {
        return orderMemSize;
    }

    @SuppressWarnings("unused")
    public void setOrderMemSize(int orderMemSize) {
        if (orderMemSize > 0) {
            this.orderMemSize = orderMemSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "orderMemSize", orderMemSize, this.orderMemSize));
        }
    }

    public int getJoinMemSize() {
        return joinMemSize;
    }

    @SuppressWarnings("unused")
    public void setJoinMemSize(int joinMemSize) {
        if (joinMemSize > 0) {
            this.joinMemSize = joinMemSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "joinMemSize", joinMemSize, this.joinMemSize));
        }
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    @SuppressWarnings("unused")
    public void setMappedFileSize(int mappedFileSize) {
        if (mappedFileSize > 0) {
            this.mappedFileSize = mappedFileSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "mappedFileSize", mappedFileSize, this.mappedFileSize));
        }
    }

    public int getNestLoopConnSize() {
        return nestLoopConnSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopConnSize(int nestLoopConnSize) {
        if (nestLoopConnSize > 0) {
            this.nestLoopConnSize = nestLoopConnSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "nestLoopConnSize", nestLoopConnSize, this.nestLoopConnSize));
        }
    }

    public int getOrderByQueueSize() {
        return orderByQueueSize;
    }

    @SuppressWarnings("unused")
    public void setOrderByQueueSize(int orderByQueueSize) {
        if (orderByQueueSize > 0) {
            this.orderByQueueSize = orderByQueueSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "orderByQueueSize", orderByQueueSize, this.orderByQueueSize));
        }
    }


    public String getViewPersistenceConfBaseDir() {
        return viewPersistenceConfBaseDir;
    }

    @SuppressWarnings("unused")
    public void setViewPersistenceConfBaseDir(String viewPersistenceConfBaseDir) {
        this.viewPersistenceConfBaseDir = viewPersistenceConfBaseDir;
    }

    public String getViewPersistenceConfBaseName() {
        return viewPersistenceConfBaseName;
    }

    @SuppressWarnings("unused")
    public void setViewPersistenceConfBaseName(String viewPersistenceConfBaseName) {
        this.viewPersistenceConfBaseName = viewPersistenceConfBaseName;
    }

    public int getUseCostTimeStat() {
        return useCostTimeStat;
    }

    @SuppressWarnings("unused")
    public void setUseCostTimeStat(int useCostTimeStat) {
        if (useCostTimeStat >= 0 && useCostTimeStat <= 1) {
            this.useCostTimeStat = useCostTimeStat;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "useCostTimeStat", useCostTimeStat, this.useCostTimeStat));
        }
    }

    public int getMaxCostStatSize() {
        return maxCostStatSize;
    }

    @SuppressWarnings("unused")
    public void setMaxCostStatSize(int maxCostStatSize) {
        if (maxCostStatSize > 0) {
            this.maxCostStatSize = maxCostStatSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxCostStatSize", maxCostStatSize, this.maxCostStatSize));
        }
    }

    public int getCostSamplePercent() {
        return costSamplePercent;
    }

    @SuppressWarnings("unused")
    public void setCostSamplePercent(int costSamplePercent) {
        if (costSamplePercent >= 0 && costSamplePercent <= 100) {
            this.costSamplePercent = costSamplePercent;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "costSamplePercent", costSamplePercent, this.costSamplePercent));
        }
    }

    public int getUseThreadUsageStat() {
        return useThreadUsageStat;
    }

    @SuppressWarnings("unused")
    public void setUseThreadUsageStat(int useThreadUsageStat) {
        if (useThreadUsageStat >= 0 && useThreadUsageStat <= 1) {
            this.useThreadUsageStat = useThreadUsageStat;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "useThreadUsageStat", useThreadUsageStat, this.useThreadUsageStat));
        }
    }


    public int getUsePerformanceMode() {
        return usePerformanceMode;
    }

    @SuppressWarnings("unused")
    public void setUsePerformanceMode(int usePerformanceMode) {
        if (usePerformanceMode >= 0 && usePerformanceMode <= 1) {
            this.usePerformanceMode = usePerformanceMode;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "usePerformanceMode", usePerformanceMode, this.usePerformanceMode));
        }
    }

    public int getUseSerializableMode() {
        return useSerializableMode;
    }

    @SuppressWarnings("unused")
    public void setUseSerializableMode(int useSerializableMode) {
        if (useSerializableMode >= 0 && useSerializableMode <= 1) {
            this.useSerializableMode = useSerializableMode;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "useSerializableMode", useSerializableMode, this.useSerializableMode));
        }
    }


    public int getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    @SuppressWarnings("unused")
    public void setWriteToBackendExecutor(int writeToBackendExecutor) {
        this.writeToBackendExecutor = writeToBackendExecutor;
    }

    public int getEnableSlowLog() {
        return enableSlowLog;
    }

    @SuppressWarnings("unused")
    public void setEnableSlowLog(int enableSlowLog) {
        if (enableSlowLog >= 0 && enableSlowLog <= 1) {
            this.enableSlowLog = enableSlowLog;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "enableSlowLog", enableSlowLog, this.enableSlowLog));
        }
    }

    public String getSlowLogBaseDir() {
        return slowLogBaseDir;
    }

    @SuppressWarnings("unused")
    public void setSlowLogBaseDir(String slowLogBaseDir) {
        this.slowLogBaseDir = slowLogBaseDir;
    }

    public String getSlowLogBaseName() {
        return slowLogBaseName;
    }

    @SuppressWarnings("unused")
    public void setSlowLogBaseName(String slowLogBaseName) {
        this.slowLogBaseName = slowLogBaseName;
    }

    public int getFlushSlowLogPeriod() {
        return flushSlowLogPeriod;
    }

    @SuppressWarnings("unused")
    public void setFlushSlowLogPeriod(int flushSlowLogPeriod) {
        if (flushSlowLogPeriod > 0) {
            this.flushSlowLogPeriod = flushSlowLogPeriod;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "flushSlowLogPeriod", flushSlowLogPeriod, this.flushSlowLogPeriod));
        }
    }

    public int getFlushSlowLogSize() {
        return flushSlowLogSize;
    }

    @SuppressWarnings("unused")
    public void setFlushSlowLogSize(int flushSlowLogSize) {
        if (flushSlowLogSize > 0) {
            this.flushSlowLogSize = flushSlowLogSize;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "flushSlowLogSize", flushSlowLogSize, this.flushSlowLogSize));
        }
    }

    public int getSqlSlowTime() {
        return sqlSlowTime;
    }

    @SuppressWarnings("unused")
    public void setSqlSlowTime(int sqlSlowTime) {
        if (sqlSlowTime > 0) {
            this.sqlSlowTime = sqlSlowTime;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "sqlSlowTime", sqlSlowTime, this.sqlSlowTime));
        }
    }

    public int getEnableAlert() {
        return enableAlert;
    }

    @SuppressWarnings("unused")
    public void setEnableAlert(int enableAlert) {
        if (enableAlert >= 0 && enableAlert <= 1) {
            this.enableAlert = enableAlert;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "enableAlert", enableAlert, this.enableAlert));
        }
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        if (maxCon >= 0) {
            this.maxCon = maxCon;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxCon", maxCon, this.maxCon));
        }
    }

    public int getMaxCharsPerColumn() {
        return maxCharsPerColumn;
    }

    @SuppressWarnings("unused")
    public void setMaxCharsPerColumn(int maxCharsPerColumn) {
        if (maxCharsPerColumn > 0 && maxCharsPerColumn <= 7 * 1024 * 256) {
            this.maxCharsPerColumn = maxCharsPerColumn;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxCharsPerColumn", maxCharsPerColumn, this.maxCharsPerColumn));
        }
    }

    public int getMaxRowSizeToFile() {
        return maxRowSizeToFile;
    }

    @SuppressWarnings("unused")
    public void setMaxRowSizeToFile(int maxRowSizeToFile) {
        if (maxRowSizeToFile > 0) {
            this.maxRowSizeToFile = maxRowSizeToFile;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "maxRowSizeToFile", maxRowSizeToFile, this.maxRowSizeToFile));
        }
    }


    public boolean isUseOuterHa() {
        return useOuterHa;
    }

    @SuppressWarnings("unused")
    public void setUseOuterHa(boolean useOuterHa) {
        this.useOuterHa = useOuterHa;
    }

    public int getXaRetryCount() {
        return xaRetryCount;
    }

    @SuppressWarnings("unused")
    public void setXaRetryCount(int xaRetryCount) {
        if (xaRetryCount >= 0) {
            this.xaRetryCount = xaRetryCount;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMATE, "xaRetryCount", xaRetryCount, this.xaRetryCount));
        }
    }


    @Override
    public String toString() {
        return "SystemConfig [" +
                ", bindIp=" + bindIp +
                ", serverPort=" + serverPort +
                ", managerPort=" + managerPort +
                ", processors=" + processors +
                ", backendProcessors=" + backendProcessors +
                ", processorExecutor=" + processorExecutor +
                ", backendProcessorExecutor=" + backendProcessorExecutor +
                ", complexExecutor=" + complexExecutor +
                ", writeToBackendExecutor=" + writeToBackendExecutor +
                ", fakeMySQLVersion=" + fakeMySQLVersion +
                ", sequnceHandlerType=" + sequnceHandlerType +
                ", serverBacklog=" + serverBacklog +
                ", serverNodeId=" + serverNodeId +
                ", showBinlogStatusTimeout=" + showBinlogStatusTimeout +
                ", maxCon=" + maxCon +
                ", useCompression=" + useCompression +
                ", usingAIO=" + usingAIO +
                ", useThreadUsageStat=" + useThreadUsageStat +
                ", usePerformanceMode=" + usePerformanceMode +
                ", useSerializableMode=" + useSerializableMode +
                ", useCostTimeStat=" + useCostTimeStat +
                ", maxCostStatSize=" + maxCostStatSize +
                ", costSamplePercent=" + costSamplePercent +
                ", charset=" + charset +
                ", maxPacketSize=" + maxPacketSize +
                ", autocommit=" + autocommit +
                ", txIsolation=" + txIsolation +
                ", checkTableConsistency=" + checkTableConsistency +
                ", checkTableConsistencyPeriod=" + checkTableConsistencyPeriod +
                ", dataNodeIdleCheckPeriod=" + dataNodeIdleCheckPeriod +
                ", dataNodeHeartbeatPeriod=" + dataNodeHeartbeatPeriod +
                ", processorCheckPeriod=" + processorCheckPeriod +
                ", idleTimeout=" + idleTimeout +
                ", sqlExecuteTimeout=" + sqlExecuteTimeout +
                ", recordTxn=" + recordTxn +
                ", transactionLogBaseDir=" + transactionLogBaseDir +
                ", transactionLogBaseName=" + transactionLogBaseName +
                ", xaRecoveryLogBaseDir=" + xaRecoveryLogBaseDir +
                ", xaRecoveryLogBaseName=" + xaRecoveryLogBaseName +
                ", xaSessionCheckPeriod=" + xaSessionCheckPeriod +
                ", xaLogCleanPeriod=" + xaLogCleanPeriod +
                ", useJoinStrategy=" + useJoinStrategy +
                ", nestLoopConnSize=" + nestLoopConnSize +
                ", nestLoopRowsSize=" + nestLoopRowsSize +
                ", otherMemSize=" + otherMemSize +
                ", orderMemSize=" + orderMemSize +
                ", joinMemSize=" + joinMemSize +
                ", bufferPoolChunkSize=" + bufferPoolChunkSize +
                ", bufferPoolPageSize=" + bufferPoolPageSize +
                ", bufferPoolPageNumber=" + bufferPoolPageNumber +
                ", useSqlStat=" + useSqlStat +
                ", sqlRecordCount=" + sqlRecordCount +
                ", maxResultSet=" + maxResultSet +
                ", bufferUsagePercent=" + bufferUsagePercent +
                ", clearBigSqLResultSetMapMs=" + clearBigSqLResultSetMapMs +
                "  frontSocketSoRcvbuf=" + frontSocketSoRcvbuf +
                ", frontSocketSoSndbuf=" + frontSocketSoSndbuf +
                ", frontSocketNoDelay=" + frontSocketNoDelay +
                ", backSocketSoRcvbuf=" + backSocketSoRcvbuf +
                ", backSocketSoSndbuf=" + backSocketSoSndbuf +
                ", backSocketNoDelay=" + backSocketNoDelay +
                ", viewPersistenceConfBaseDir=" + viewPersistenceConfBaseDir +
                ", viewPersistenceConfBaseName=" + viewPersistenceConfBaseName +
                ", joinQueueSize=" + joinQueueSize +
                ", mergeQueueSize=" + mergeQueueSize +
                ", orderByQueueSize=" + orderByQueueSize +
                ", enableSlowLog=" + enableSlowLog +
                ", slowLogBaseDir=" + slowLogBaseDir +
                ", slowLogBaseName=" + slowLogBaseName +
                ", flushSlowLogPeriod=" + flushSlowLogPeriod +
                ", flushSlowLogSize=" + flushSlowLogSize +
                ", sqlSlowTime=" + sqlSlowTime +
                ", enableAlert=" + enableAlert +
                ", maxCharsPerColumn=" + maxCharsPerColumn +
                ", maxRowSizeToFile=" + maxRowSizeToFile +
                ", xaRetryCount=" + xaRetryCount +
                "]";
    }

}
