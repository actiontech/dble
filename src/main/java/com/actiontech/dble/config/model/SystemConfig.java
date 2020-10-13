/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.util.NetUtil;

import java.io.File;
import java.io.IOException;

import static com.actiontech.dble.config.model.db.PoolConfig.DEFAULT_IDLE_TIMEOUT;


public final class SystemConfig {
    private ProblemReporter problemReporter = StartProblemReporter.getInstance();
    private static final SystemConfig INSTANCE = new SystemConfig();

    public static SystemConfig getInstance() {
        return INSTANCE;
    }

    private SystemConfig() {
    }

    private static final String WARNING_FORMAT = "Property [ %s ] '%d' in bootstrap.cnf is illegal, you may need use the default value %d replaced";

    /*
     * the supported  protocol version of MySQL
     * For Other MySQL branch ,like MariaDB 10.1.x,
     * but its protocol is compatible with MySQL. So the versions array only contain official version here
     */
    public static final String[] MYSQL_VERSIONS = {"5.5", "5.6", "5.7", "8.0"};
    // base config
    private String homePath = null;
    private String serverId = NetUtil.getHostIp();
    private String instanceName = null;
    private int instanceId = -1;

    private String bindIp = "0.0.0.0";
    private int serverPort = 8066;
    private int managerPort = 9066;
    private int processors = Runtime.getRuntime().availableProcessors();
    private int backendProcessors = processors;
    private int processorExecutor = (processors != 1) ? processors : 2;
    private int backendProcessorExecutor = (processors != 1) ? processors : 2;
    private int complexExecutor = processorExecutor > 8 ? 8 : processorExecutor;
    private int writeToBackendExecutor = (processors != 1) ? processors : 2;
    private int serverBacklog = 2048;
    private int maxCon = 0;
    //option
    private int useCompression = 0;
    private boolean capClientFoundRows = false;
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

    //processor check conn
    private long processorCheckPeriod = 1000L;
    //front conn idle timeout
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    // sql execute timeout (second)
    private long sqlExecuteTimeout = 300;

    //transaction log
    private int recordTxn = 0;
    private String transactionLogBaseDir = "txlogs";
    private String transactionLogBaseName = "server-tx";
    private int transactionRotateSize = 16; // M

    //XA transaction
    private long xaSessionCheckPeriod = 1000L;
    private long xaLogCleanPeriod = 1000L;
    private String xaRecoveryLogBaseDir = "xalogs";
    private String xaRecoveryLogBaseName = "xalog";
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
    private long clearBigSQLResultSetMapMs = 10 * 60 * 1000;

    //frontSocket unit:bytes
    private int frontSocketSoRcvbuf = 1024 * 1024;
    private int frontSocketSoSndbuf = 4 * 1024 * 1024;
    private int frontSocketNoDelay = 1; // 0=false

    // backSocket unit:bytes
    private int backSocketSoRcvbuf = 4 * 1024 * 1024;
    private int backSocketSoSndbuf = 1024 * 1024;
    private int backSocketNoDelay = 1; // 1=true

    //view
    private String viewPersistenceConfBaseDir = "viewConf";
    private String viewPersistenceConfBaseName = "viewJson";

    // for join tmp results
    private int mergeQueueSize = 1024;
    private int orderByQueueSize = 1024;
    private int joinQueueSize = 1024;
    //slow log
    private int enableSlowLog = 0;
    private String slowLogBaseDir = "slowlogs";
    private String slowLogBaseName = "slow-query";
    private int flushSlowLogPeriod = 1; //second
    private int flushSlowLogSize = 1000; //row
    private int sqlSlowTime = 100; //ms
    //alert switch
    private int enableAlert = 1;
    //load data
    private int maxRowSizeToFile = 10000;
    private int maxCharsPerColumn = 65535; // 128k,65535 chars

    private boolean enableFlowControl = false;
    private int flowControlStartThreshold = 4096;
    private int flowControlStopThreshold = 256;
    private boolean useOuterHa = true;
    private String traceEndPoint = null;
    private String fakeMySQLVersion = "5.7.21";

    public int getTransactionRotateSize() {
        return transactionRotateSize;
    }

    @SuppressWarnings("unused")
    public void setTransactionRotateSize(int transactionRotateSize) {
        if (transactionRotateSize > 0) {
            this.transactionRotateSize = transactionRotateSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "transactionRotateSize", transactionRotateSize, this.transactionRotateSize));
        }
    }

    public String getTransactionLogBaseDir() {
        return (this.getHomePath() + File.separatorChar + transactionLogBaseDir + File.separatorChar).replaceAll(File.separator + "+", File.separator);
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
        return (this.getHomePath() + File.separatorChar + xaRecoveryLogBaseDir + File.separatorChar).replaceAll(File.separator + "+", File.separator);
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


    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    @SuppressWarnings("unused")
    public void setMaxPacketSize(int maxPacketSize) {
        if (maxPacketSize >= 1024 && maxPacketSize <= 1073741824) {
            this.maxPacketSize = maxPacketSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxPacketSize", maxPacketSize, this.maxPacketSize));
        }
    }

    public String getBindIp() {
        return bindIp;
    }

    @SuppressWarnings("unused")
    public void setBindIp(String bindIp) {
        this.bindIp = bindIp;
    }

    @SuppressWarnings("unused")
    public void setHomePath(String homePath) {
        if (homePath != null && homePath.endsWith(File.pathSeparator)) {
            homePath = homePath.substring(0, homePath.length() - 1);
        }
        this.homePath = homePath;
    }

    public String getHomePath() {
        // if HOME is not set,set it as current path or parent path
        if (this.homePath == null) {
            try {
                String path = new File("..").getCanonicalPath().replaceAll("\\\\", "/");
                File conf = new File(path + "/conf");
                if (conf.exists() && conf.isDirectory()) {
                    homePath = path;
                } else {
                    path = new File(".").getCanonicalPath().replaceAll("\\\\", "/");
                    conf = new File(path + "/conf");
                    if (conf.exists() && conf.isDirectory()) {
                        homePath = path;
                    }
                }
            } catch (IOException e) {
                //ignore error
            }
        }

        return homePath;
    }

    public int getUseSqlStat() {
        return useSqlStat;
    }

    @SuppressWarnings("unused")
    public void setUseSqlStat(int useSqlStat) {
        if (useSqlStat >= 0 && useSqlStat <= 1) {
            this.useSqlStat = useSqlStat;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "useSqlStat", useSqlStat, this.useSqlStat));
        }
    }

    public int getUseCompression() {
        return useCompression;
    }

    @SuppressWarnings("unused")
    public void setUseCompression(int useCompression) {
        if (useCompression >= 0 && useCompression <= 1) {
            this.useCompression = useCompression;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "useCompression", useCompression, this.useCompression));
        }
    }

    public boolean isCapClientFoundRows() {
        return capClientFoundRows;
    }

    public void setCapClientFoundRows(boolean capClientFoundRows) {
        this.capClientFoundRows = capClientFoundRows;
    }

    public String getCharset() {
        return charset;
    }

    @SuppressWarnings("unused")
    public void setCharset(String charset) {
        if (CharsetUtil.getCharsetDefaultIndex(charset) > 0) {
            this.charset = charset;
        } else {
            problemReporter.warn("Property [ charset ] '" + charset + "' in bootstrap.cnf is illegal, use " + this.charset + " replaced");
        }
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
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "processors", processors, this.processors));
        }
    }

    public int getBackendProcessors() {
        return backendProcessors;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessors(int backendProcessors) {
        if (backendProcessors > 0) {
            this.backendProcessors = backendProcessors;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "backendProcessors", backendProcessors, this.backendProcessors));
        }
    }

    public int getProcessorExecutor() {
        return processorExecutor;
    }

    @SuppressWarnings("unused")
    public void setProcessorExecutor(int processorExecutor) {
        if (processorExecutor > 0) {
            this.processorExecutor = processorExecutor;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "processorExecutor", processorExecutor, this.processorExecutor));
        }
    }

    public int getBackendProcessorExecutor() {
        return backendProcessorExecutor;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessorExecutor(int backendProcessorExecutor) {
        if (backendProcessorExecutor > 0) {
            this.backendProcessorExecutor = backendProcessorExecutor;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "backendProcessorExecutor", backendProcessorExecutor, this.backendProcessorExecutor));
        }
    }

    public int getComplexExecutor() {
        return complexExecutor;
    }

    @SuppressWarnings("unused")
    public void setComplexExecutor(int complexExecutor) {
        if (complexExecutor > 0) {
            this.complexExecutor = complexExecutor;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "complexExecutor", complexExecutor, this.complexExecutor));
        }
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        if (processorCheckPeriod > 0) {
            this.processorCheckPeriod = processorCheckPeriod;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "processorCheckPeriod", processorCheckPeriod, this.processorCheckPeriod));
        }
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    @SuppressWarnings("unused")
    public void setIdleTimeout(long idleTimeout) {
        if (idleTimeout > 0) {
            this.idleTimeout = idleTimeout;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "idleTimeout", idleTimeout, this.idleTimeout));
        }
    }

    public long getXaSessionCheckPeriod() {
        return xaSessionCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaSessionCheckPeriod(long xaSessionCheckPeriod) {
        if (xaSessionCheckPeriod > 0) {
            this.xaSessionCheckPeriod = xaSessionCheckPeriod;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "xaSessionCheckPeriod", xaSessionCheckPeriod, this.xaSessionCheckPeriod));
        }
    }

    public long getXaLogCleanPeriod() {
        return xaLogCleanPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaLogCleanPeriod(long xaLogCleanPeriod) {
        if (xaLogCleanPeriod > 0) {
            this.xaLogCleanPeriod = xaLogCleanPeriod;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "xaLogCleanPeriod", xaLogCleanPeriod, this.xaLogCleanPeriod));
        }
    }

    public long getSqlExecuteTimeout() {
        return sqlExecuteTimeout;
    }

    @SuppressWarnings("unused")
    public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
        if (sqlExecuteTimeout > 0) {
            this.sqlExecuteTimeout = sqlExecuteTimeout;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "sqlExecuteTimeout", sqlExecuteTimeout, this.sqlExecuteTimeout));
        }
    }


    public int getTxIsolation() {
        return txIsolation;
    }

    @SuppressWarnings("unused")
    public void setTxIsolation(int txIsolation) {
        if (txIsolation >= 1 && txIsolation <= 4) {
            this.txIsolation = txIsolation;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "txIsolation", txIsolation, this.txIsolation));
        }
    }

    public int getAutocommit() {
        return autocommit;
    }

    @SuppressWarnings("unused")
    public void setAutocommit(int autocommit) {
        if (autocommit >= 0 && autocommit <= 1) {
            this.autocommit = autocommit;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "autocommit", autocommit, this.autocommit));
        }
    }


    public int getSqlRecordCount() {
        return sqlRecordCount;
    }

    @SuppressWarnings("unused")
    public void setSqlRecordCount(int sqlRecordCount) {
        if (sqlRecordCount > 0) {
            this.sqlRecordCount = sqlRecordCount;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "sqlRecordCount", sqlRecordCount, this.sqlRecordCount));
        }
    }

    public int getRecordTxn() {
        return recordTxn;
    }

    @SuppressWarnings("unused")
    public void setRecordTxn(int recordTxn) {
        if (recordTxn >= 0 && recordTxn <= 1) {
            this.recordTxn = recordTxn;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "recordTxn", recordTxn, this.recordTxn));
        }
    }

    public short getBufferPoolChunkSize() {
        return bufferPoolChunkSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
        if (bufferPoolChunkSize > 0) {
            this.bufferPoolChunkSize = bufferPoolChunkSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "bufferPoolChunkSize", bufferPoolChunkSize, this.bufferPoolChunkSize));
        }
    }

    public int getMaxResultSet() {
        return maxResultSet;
    }

    @SuppressWarnings("unused")
    public void setMaxResultSet(int maxResultSet) {
        if (maxResultSet > 0) {
            this.maxResultSet = maxResultSet;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxResultSet", maxResultSet, this.maxResultSet));
        }
    }

    public int getBufferUsagePercent() {
        return bufferUsagePercent;
    }

    @SuppressWarnings("unused")
    public void setBufferUsagePercent(int bufferUsagePercent) {
        if (bufferUsagePercent >= 0 && bufferUsagePercent <= 100) {
            this.bufferUsagePercent = bufferUsagePercent;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "bufferUsagePercent", bufferUsagePercent, this.bufferUsagePercent));
        }
    }

    public long getClearBigSQLResultSetMapMs() {
        return clearBigSQLResultSetMapMs;
    }

    @SuppressWarnings("unused")
    public void setClearBigSQLResultSetMapMs(long clearBigSQLResultSetMapMs) {
        if (clearBigSQLResultSetMapMs > 0) {
            this.clearBigSQLResultSetMapMs = clearBigSQLResultSetMapMs;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "clearBigSQLResultSetMapMs", clearBigSQLResultSetMapMs, this.clearBigSQLResultSetMapMs));
        }
    }

    public int getBufferPoolPageSize() {
        return bufferPoolPageSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolPageSize(int bufferPoolPageSize) {
        if (bufferPoolPageSize > 0) {
            this.bufferPoolPageSize = bufferPoolPageSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "bufferPoolPageSize", bufferPoolPageSize, this.bufferPoolPageSize));
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
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "bufferPoolPageNumber", bufferPoolPageNumber, this.bufferPoolPageNumber));
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
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "frontSocketSoRcvbuf", frontSocketSoRcvbuf, this.frontSocketSoRcvbuf));
        }
    }

    public int getFrontSocketSoSndbuf() {
        return frontSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
        if (frontSocketSoSndbuf > 0) {
            this.frontSocketSoSndbuf = frontSocketSoSndbuf;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "frontSocketSoSndbuf", frontSocketSoSndbuf, this.frontSocketSoSndbuf));
        }
    }

    public int getBackSocketSoRcvbuf() {
        return backSocketSoRcvbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
        if (backSocketSoRcvbuf > 0) {
            this.backSocketSoRcvbuf = backSocketSoRcvbuf;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "backSocketSoRcvbuf", backSocketSoRcvbuf, this.backSocketSoRcvbuf));
        }
    }

    public int getBackSocketSoSndbuf() {
        return backSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
        if (backSocketSoSndbuf > 0) {
            this.backSocketSoSndbuf = backSocketSoSndbuf;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "backSocketSoSndbuf", backSocketSoSndbuf, this.backSocketSoSndbuf));
        }
    }

    public int getFrontSocketNoDelay() {
        return frontSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketNoDelay(int frontSocketNoDelay) {
        if (frontSocketNoDelay >= 0 && frontSocketNoDelay <= 1) {
            this.frontSocketNoDelay = frontSocketNoDelay;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "frontSocketNoDelay", frontSocketNoDelay, this.frontSocketNoDelay));
        }
    }

    public int getBackSocketNoDelay() {
        return backSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setBackSocketNoDelay(int backSocketNoDelay) {
        if (backSocketNoDelay >= 0 && backSocketNoDelay <= 1) {
            this.backSocketNoDelay = backSocketNoDelay;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "backSocketNoDelay", backSocketNoDelay, this.backSocketNoDelay));
        }
    }

    public int getUsingAIO() {
        return usingAIO;
    }

    @SuppressWarnings("unused")
    public void setUsingAIO(int usingAIO) {
        if (usingAIO >= 0 && usingAIO <= 1) {
            this.usingAIO = usingAIO;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "usingAIO", usingAIO, this.usingAIO));
        }
    }


    public String getServerId() {
        return serverId;
    }

    @SuppressWarnings("unused")
    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getInstanceName() {
        return instanceName;
    }

    @SuppressWarnings("unused")
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }


    public int getInstanceId() {
        return instanceId;
    }

    @SuppressWarnings("unused")
    public void setInstanceId(int instanceId) {
        this.instanceId = instanceId;
    }

    public int getCheckTableConsistency() {
        return checkTableConsistency;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistency(int checkTableConsistency) {
        if (checkTableConsistency >= 0 && checkTableConsistency <= 1) {
            this.checkTableConsistency = checkTableConsistency;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "checkTableConsistency", checkTableConsistency, this.checkTableConsistency));
        }
    }

    public long getCheckTableConsistencyPeriod() {
        return checkTableConsistencyPeriod;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistencyPeriod(long checkTableConsistencyPeriod) {
        if (checkTableConsistencyPeriod > 0) {
            this.checkTableConsistencyPeriod = checkTableConsistencyPeriod;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "checkTableConsistencyPeriod", checkTableConsistencyPeriod, this.checkTableConsistencyPeriod));
        }
    }

    public int getNestLoopRowsSize() {
        return nestLoopRowsSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopRowsSize(int nestLoopRowsSize) {
        if (nestLoopRowsSize > 0) {
            this.nestLoopRowsSize = nestLoopRowsSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "nestLoopRowsSize", nestLoopRowsSize, this.nestLoopRowsSize));
        }
    }

    public int getJoinQueueSize() {
        return joinQueueSize;
    }

    @SuppressWarnings("unused")
    public void setJoinQueueSize(int joinQueueSize) {
        if (joinQueueSize > 0) {
            this.joinQueueSize = joinQueueSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "joinQueueSize", joinQueueSize, this.joinQueueSize));
        }
    }

    public int getMergeQueueSize() {
        return mergeQueueSize;
    }

    @SuppressWarnings("unused")
    public void setMergeQueueSize(int mergeQueueSize) {
        if (mergeQueueSize > 0) {
            this.mergeQueueSize = mergeQueueSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "mergeQueueSize", mergeQueueSize, this.mergeQueueSize));
        }
    }

    public int getOtherMemSize() {
        return otherMemSize;
    }

    @SuppressWarnings("unused")
    public void setOtherMemSize(int otherMemSize) {
        if (otherMemSize > 0) {
            this.otherMemSize = otherMemSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "otherMemSize", otherMemSize, this.otherMemSize));
        }
    }

    public int getOrderMemSize() {
        return orderMemSize;
    }

    @SuppressWarnings("unused")
    public void setOrderMemSize(int orderMemSize) {
        if (orderMemSize > 0) {
            this.orderMemSize = orderMemSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "orderMemSize", orderMemSize, this.orderMemSize));
        }
    }

    public int getJoinMemSize() {
        return joinMemSize;
    }

    @SuppressWarnings("unused")
    public void setJoinMemSize(int joinMemSize) {
        if (joinMemSize > 0) {
            this.joinMemSize = joinMemSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "joinMemSize", joinMemSize, this.joinMemSize));
        }
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    @SuppressWarnings("unused")
    public void setMappedFileSize(int mappedFileSize) {
        if (mappedFileSize > 0) {
            this.mappedFileSize = mappedFileSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "mappedFileSize", mappedFileSize, this.mappedFileSize));
        }
    }

    public int getNestLoopConnSize() {
        return nestLoopConnSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopConnSize(int nestLoopConnSize) {
        if (nestLoopConnSize > 0) {
            this.nestLoopConnSize = nestLoopConnSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "nestLoopConnSize", nestLoopConnSize, this.nestLoopConnSize));
        }
    }

    public int getOrderByQueueSize() {
        return orderByQueueSize;
    }

    @SuppressWarnings("unused")
    public void setOrderByQueueSize(int orderByQueueSize) {
        if (orderByQueueSize > 0) {
            this.orderByQueueSize = orderByQueueSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "orderByQueueSize", orderByQueueSize, this.orderByQueueSize));
        }
    }


    public String getViewPersistenceConfBaseDir() {
        return (this.getHomePath() + File.separatorChar + viewPersistenceConfBaseDir + File.separatorChar).replaceAll(File.separator + "+", File.separator);
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
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "useCostTimeStat", useCostTimeStat, this.useCostTimeStat));
        }
    }

    public int getMaxCostStatSize() {
        return maxCostStatSize;
    }

    @SuppressWarnings("unused")
    public void setMaxCostStatSize(int maxCostStatSize) {
        if (maxCostStatSize > 0) {
            this.maxCostStatSize = maxCostStatSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxCostStatSize", maxCostStatSize, this.maxCostStatSize));
        }
    }

    public int getCostSamplePercent() {
        return costSamplePercent;
    }

    @SuppressWarnings("unused")
    public void setCostSamplePercent(int costSamplePercent) {
        if (costSamplePercent >= 0 && costSamplePercent <= 100) {
            this.costSamplePercent = costSamplePercent;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "costSamplePercent", costSamplePercent, this.costSamplePercent));
        }
    }

    public int getUseThreadUsageStat() {
        return useThreadUsageStat;
    }

    @SuppressWarnings("unused")
    public void setUseThreadUsageStat(int useThreadUsageStat) {
        if (useThreadUsageStat >= 0 && useThreadUsageStat <= 1) {
            this.useThreadUsageStat = useThreadUsageStat;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "useThreadUsageStat", useThreadUsageStat, this.useThreadUsageStat));
        }
    }


    public int getUsePerformanceMode() {
        return usePerformanceMode;
    }

    @SuppressWarnings("unused")
    public void setUsePerformanceMode(int usePerformanceMode) {
        if (usePerformanceMode >= 0 && usePerformanceMode <= 1) {
            this.usePerformanceMode = usePerformanceMode;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "usePerformanceMode", usePerformanceMode, this.usePerformanceMode));
        }
    }

    public int getUseSerializableMode() {
        return useSerializableMode;
    }

    @SuppressWarnings("unused")
    public void setUseSerializableMode(int useSerializableMode) {
        if (useSerializableMode >= 0 && useSerializableMode <= 1) {
            this.useSerializableMode = useSerializableMode;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "useSerializableMode", useSerializableMode, this.useSerializableMode));
        }
    }


    public int getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    @SuppressWarnings("unused")
    public void setWriteToBackendExecutor(int writeToBackendExecutor) {
        if (writeToBackendExecutor > 0) {
            this.writeToBackendExecutor = writeToBackendExecutor;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "writeToBackendExecutor", writeToBackendExecutor, this.writeToBackendExecutor));
        }
    }

    public int getEnableSlowLog() {
        return enableSlowLog;
    }

    @SuppressWarnings("unused")
    public void setEnableSlowLog(int enableSlowLog) {
        if (enableSlowLog >= 0 && enableSlowLog <= 1) {
            this.enableSlowLog = enableSlowLog;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableSlowLog", enableSlowLog, this.enableSlowLog));
        }
    }

    public String getSlowLogBaseDir() {
        return (this.getHomePath() + File.separatorChar + slowLogBaseDir + File.separatorChar).replaceAll(File.separator + "+", File.separator);
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
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "flushSlowLogPeriod", flushSlowLogPeriod, this.flushSlowLogPeriod));
        }
    }

    public int getFlushSlowLogSize() {
        return flushSlowLogSize;
    }

    @SuppressWarnings("unused")
    public void setFlushSlowLogSize(int flushSlowLogSize) {
        if (flushSlowLogSize > 0) {
            this.flushSlowLogSize = flushSlowLogSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "flushSlowLogSize", flushSlowLogSize, this.flushSlowLogSize));
        }
    }

    public int getSqlSlowTime() {
        return sqlSlowTime;
    }

    @SuppressWarnings("unused")
    public void setSqlSlowTime(int sqlSlowTime) {
        if (sqlSlowTime >= 0) {
            this.sqlSlowTime = sqlSlowTime;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "sqlSlowTime", sqlSlowTime, this.sqlSlowTime));
        }
    }

    public int getEnableAlert() {
        return enableAlert;
    }

    @SuppressWarnings("unused")
    public void setEnableAlert(int enableAlert) {
        if (enableAlert >= 0 && enableAlert <= 1) {
            this.enableAlert = enableAlert;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableAlert", enableAlert, this.enableAlert));
        }
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        if (maxCon >= 0) {
            this.maxCon = maxCon;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxCon", maxCon, this.maxCon));
        }
    }

    public int getMaxCharsPerColumn() {
        return maxCharsPerColumn;
    }

    @SuppressWarnings("unused")
    public void setMaxCharsPerColumn(int maxCharsPerColumn) {
        if (maxCharsPerColumn > 0 && maxCharsPerColumn <= 7 * 1024 * 256) {
            this.maxCharsPerColumn = maxCharsPerColumn;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxCharsPerColumn", maxCharsPerColumn, this.maxCharsPerColumn));
        }
    }

    public int getMaxRowSizeToFile() {
        return maxRowSizeToFile;
    }

    @SuppressWarnings("unused")
    public void setMaxRowSizeToFile(int maxRowSizeToFile) {
        if (maxRowSizeToFile > 0) {
            this.maxRowSizeToFile = maxRowSizeToFile;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxRowSizeToFile", maxRowSizeToFile, this.maxRowSizeToFile));
        }
    }

    public int getXaRetryCount() {
        return xaRetryCount;
    }

    @SuppressWarnings("unused")
    public void setXaRetryCount(int xaRetryCount) {
        if (xaRetryCount >= 0) {
            this.xaRetryCount = xaRetryCount;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "xaRetryCount", xaRetryCount, this.xaRetryCount));
        }
    }

    public boolean isEnableFlowControl() {
        return enableFlowControl;
    }

    @SuppressWarnings("unused")
    public void setEnableFlowControl(boolean enableFlowControl) {
        this.enableFlowControl = enableFlowControl;
    }

    public int getFlowControlStartThreshold() {
        return flowControlStartThreshold;
    }

    @SuppressWarnings("unused")
    public void setFlowControlStartThreshold(int flowControlStartThreshold) {
        this.flowControlStartThreshold = flowControlStartThreshold;
    }

    public int getFlowControlStopThreshold() {
        return flowControlStopThreshold;
    }

    @SuppressWarnings("unused")
    public void setFlowControlStopThreshold(int flowControlStopThreshold) {
        this.flowControlStopThreshold = flowControlStopThreshold;
    }


    public boolean isUseOuterHa() {
        return useOuterHa;
    }

    @SuppressWarnings("unused")
    public void setUseOuterHa(boolean useOuterHa) {
        this.useOuterHa = useOuterHa;
    }


    public String getFakeMySQLVersion() {
        return fakeMySQLVersion;
    }

    @SuppressWarnings("unused")
    public void setFakeMySQLVersion(String mysqlVersion) {
        this.fakeMySQLVersion = mysqlVersion;
    }

    public String getTraceEndPoint() {
        return traceEndPoint;
    }

    @SuppressWarnings("unused")
    public void setTraceEndPoint(String traceEndPoint) {
        this.traceEndPoint = traceEndPoint;
    }

    @Override
    public String toString() {
        return "SystemConfig [" +
                ", serverId=" + serverId +
                ", instanceName=" + instanceName +
                ", instanceId=" + instanceId +
                ", bindIp=" + bindIp +
                ", serverPort=" + serverPort +
                ", managerPort=" + managerPort +
                ", processors=" + processors +
                ", backendProcessors=" + backendProcessors +
                ", processorExecutor=" + processorExecutor +
                ", backendProcessorExecutor=" + backendProcessorExecutor +
                ", complexExecutor=" + complexExecutor +
                ", writeToBackendExecutor=" + writeToBackendExecutor +
                ", serverBacklog=" + serverBacklog +
                ", maxCon=" + maxCon +
                ", useCompression=" + useCompression +
                ", capClientFoundRows=" + capClientFoundRows +
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
                ", processorCheckPeriod=" + processorCheckPeriod +
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
                ", clearBigSQLResultSetMapMs=" + clearBigSQLResultSetMapMs +
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
                ", enableFlowControl=" + enableFlowControl +
                ", flowControlStartThreshold=" + flowControlStartThreshold +
                ", flowControlStopThreshold=" + flowControlStopThreshold +
                ", useOuterHa=" + useOuterHa +
                ", fakeMySQLVersion=" + fakeMySQLVersion +
                ", traceEndPoint=" + traceEndPoint +
                "]";
    }
}
