/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Isolations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * SystemConfig
 *
 * @author mycat
 */
public final class SystemConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfig.class);
    public static final String SYS_HOME = "DBLE_HOME";
    static final long DEFAULT_IDLE_TIMEOUT = 30 * 60 * 1000L;
    public static final int SEQUENCE_HANDLER_MYSQL = 1;
    public static final int SEQUENCE_HANDLER_LOCAL_TIME = 2;
    public static final int SEQUENCE_HANDLER_ZK_DISTRIBUTED = 3;
    public static final int SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT = 4;
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
    private int maxCon = 1024;
    //option
    private int useCompression = 0;
    private int usingAIO = 0;
    private boolean useZKSwitch = true;
    private int useThreadUsageStat = 0;
    private int usePerformanceMode = 0;

    //query time cost statistics
    private int useCostTimeStat = 0;
    private int maxCostStatSize = 100;
    private int costSamplePercent = 1;
    //connection
    private String charset = "utf8";
    private int maxPacketSize = 16 * 1024 * 1024;
    private int txIsolation = Isolations.REPEATABLE_READ;

    //consistency
    private int checkTableConsistency = 0;
    private long checkTableConsistencyPeriod = 30 * 60 * 1000;
    private int useGlobleTableCheck = 1;
    private long glableTableCheckPeriod = 24 * 60 * 60 * 1000L;

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
    private int bufferPoolPageSize = 512 * 1024 * 4;
    //minimum allocation unit
    private short bufferPoolChunkSize = 4096;
    // buffer pool page number
    private short bufferPoolPageNumber = (short) (DEFAULT_PROCESSORS * 20);
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

    //cluster
    private String clusterHeartbeatUser = "_HEARTBEAT_USER_";
    private String clusterHeartbeatPass = "_HEARTBEAT_PASS_";

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

    public SystemConfig() {
    }

    public int getTransactionRatateSize() {
        return transactionRatateSize;
    }

    @SuppressWarnings("unused")
    public void setTransactionRatateSize(int transactionRatateSize) {
        this.transactionRatateSize = transactionRatateSize;
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

    public boolean isUseZKSwitch() {
        return useZKSwitch;
    }

    @SuppressWarnings("unused")
    public void setUseZKSwitch(boolean useZKSwitch) {
        this.useZKSwitch = useZKSwitch;
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

    public int getUseGlobleTableCheck() {
        return useGlobleTableCheck;
    }

    @SuppressWarnings("unused")
    public void setUseGlobleTableCheck(int useGlobleTableCheck) {
        if (useGlobleTableCheck >= 0 && useGlobleTableCheck <= 1) {
            this.useGlobleTableCheck = useGlobleTableCheck;
        } else {
            LOGGER.warn("useGlobleTableCheck " + useGlobleTableCheck + " in server.xml is not recognized ,use " + this.useGlobleTableCheck + " replaced");
        }
    }

    public long getGlableTableCheckPeriod() {
        return glableTableCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setGlableTableCheckPeriod(long glableTableCheckPeriod) {
        this.glableTableCheckPeriod = glableTableCheckPeriod;
    }

    public int getSequnceHandlerType() {
        return sequnceHandlerType;
    }

    @SuppressWarnings("unused")
    public void setSequnceHandlerType(int sequnceHandlerType) {
        if (sequnceHandlerType >= 1 && sequnceHandlerType <= 4) {
            this.sequnceHandlerType = sequnceHandlerType;
        } else {
            LOGGER.warn("sequnceHandlerType " + sequnceHandlerType + " in server.xml is not recognized ,use " + this.sequnceHandlerType + " replaced");
        }
    }


    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    @SuppressWarnings("unused")
    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
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
        } else {
            LOGGER.warn("useSqlStat " + useSqlStat + " in server.xml is not recognized ,use " + this.useSqlStat + " replaced");
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
            LOGGER.warn("useCompression " + useCompression + " in server.xml is not recognized ,use " + this.useCompression + " replaced");
        }
    }

    public String getCharset() {
        return charset;
    }

    @SuppressWarnings("unused")
    public void setCharset(String charset) {
        if (CharsetUtil.getCharsetDefaultIndex(charset) > 0) {
            this.charset = charset;
        } else {
            LOGGER.warn("Character set " + charset + " in server.xml is not recognized ,use " + this.charset + " replaced");
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
        this.processors = processors;
    }

    public int getBackendProcessors() {
        return backendProcessors;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessors(int backendProcessors) {
        this.backendProcessors = backendProcessors;
    }

    public int getProcessorExecutor() {
        return processorExecutor;
    }

    @SuppressWarnings("unused")
    public void setProcessorExecutor(int processorExecutor) {
        this.processorExecutor = processorExecutor;
    }

    public int getBackendProcessorExecutor() {
        return backendProcessorExecutor;
    }

    @SuppressWarnings("unused")
    public void setBackendProcessorExecutor(int backendProcessorExecutor) {
        this.backendProcessorExecutor = backendProcessorExecutor;
    }

    public int getComplexExecutor() {
        return complexExecutor;
    }

    @SuppressWarnings("unused")
    public void setComplexExecutor(int complexExecutor) {
        this.complexExecutor = complexExecutor;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    @SuppressWarnings("unused")
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        this.processorCheckPeriod = processorCheckPeriod;
    }

    public long getXaSessionCheckPeriod() {
        return xaSessionCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaSessionCheckPeriod(long xaSessionCheckPeriod) {
        this.xaSessionCheckPeriod = xaSessionCheckPeriod;
    }

    public long getXaLogCleanPeriod() {
        return xaLogCleanPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaLogCleanPeriod(long xaLogCleanPeriod) {
        this.xaLogCleanPeriod = xaLogCleanPeriod;
    }

    public long getDataNodeIdleCheckPeriod() {
        return dataNodeIdleCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setDataNodeIdleCheckPeriod(long dataNodeIdleCheckPeriod) {
        this.dataNodeIdleCheckPeriod = dataNodeIdleCheckPeriod;
    }

    public long getDataNodeHeartbeatPeriod() {
        return dataNodeHeartbeatPeriod;
    }

    @SuppressWarnings("unused")
    public void setDataNodeHeartbeatPeriod(long dataNodeHeartbeatPeriod) {
        this.dataNodeHeartbeatPeriod = dataNodeHeartbeatPeriod;
    }

    public String getClusterHeartbeatUser() {
        return clusterHeartbeatUser;
    }

    @SuppressWarnings("unused")
    public void setClusterHeartbeatUser(String clusterHeartbeatUser) {
        this.clusterHeartbeatUser = clusterHeartbeatUser;
    }

    public long getSqlExecuteTimeout() {
        return sqlExecuteTimeout;
    }

    @SuppressWarnings("unused")
    public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
        this.sqlExecuteTimeout = sqlExecuteTimeout;
    }


    public long getShowBinlogStatusTimeout() {
        return showBinlogStatusTimeout;
    }

    @SuppressWarnings("unused")
    public void setShowBinlogStatusTimeout(long showBinlogStatusTimeout) {
        this.showBinlogStatusTimeout = showBinlogStatusTimeout;
    }

    public String getClusterHeartbeatPass() {
        return clusterHeartbeatPass;
    }

    @SuppressWarnings("unused")
    public void setClusterHeartbeatPass(String clusterHeartbeatPass) {
        this.clusterHeartbeatPass = clusterHeartbeatPass;
    }


    public int getTxIsolation() {
        return txIsolation;
    }

    @SuppressWarnings("unused")
    public void setTxIsolation(int txIsolation) {
        if (txIsolation >= 1 && txIsolation <= 4) {
            this.txIsolation = txIsolation;
        } else {
            LOGGER.warn("txIsolation " + txIsolation + " in server.xml is not recognized ,use " + this.txIsolation + " replaced");
        }
    }

    public int getSqlRecordCount() {
        return sqlRecordCount;
    }

    @SuppressWarnings("unused")
    public void setSqlRecordCount(int sqlRecordCount) {
        this.sqlRecordCount = sqlRecordCount;
    }

    public int getRecordTxn() {
        return recordTxn;
    }

    @SuppressWarnings("unused")
    public void setRecordTxn(int recordTxn) {
        if (recordTxn >= 0 && recordTxn <= 1) {
            this.recordTxn = recordTxn;
        } else {
            LOGGER.warn("recordTxn " + recordTxn + " in server.xml is not recognized ,use " + this.recordTxn + " replaced");
        }
    }

    public short getBufferPoolChunkSize() {
        return bufferPoolChunkSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
        this.bufferPoolChunkSize = bufferPoolChunkSize;
    }

    public int getMaxResultSet() {
        return maxResultSet;
    }

    @SuppressWarnings("unused")
    public void setMaxResultSet(int maxResultSet) {
        this.maxResultSet = maxResultSet;
    }

    public int getBufferUsagePercent() {
        return bufferUsagePercent;
    }

    @SuppressWarnings("unused")
    public void setBufferUsagePercent(int bufferUsagePercent) {
        if (bufferUsagePercent >= 0 && bufferUsagePercent <= 100) {
            this.bufferUsagePercent = bufferUsagePercent;
        } else {
            LOGGER.warn("bufferUsagePercent " + bufferUsagePercent + " in server.xml is not recognized ,use " + this.bufferUsagePercent + " replaced");
        }
    }

    public long getClearBigSqLResultSetMapMs() {
        return clearBigSqLResultSetMapMs;
    }

    @SuppressWarnings("unused")
    public void setClearBigSqLResultSetMapMs(long clearBigSqLResultSetMapMs) {
        this.clearBigSqLResultSetMapMs = clearBigSqLResultSetMapMs;
    }

    public int getBufferPoolPageSize() {
        return bufferPoolPageSize;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolPageSize(int bufferPoolPageSize) {
        this.bufferPoolPageSize = bufferPoolPageSize;
    }

    public short getBufferPoolPageNumber() {
        return bufferPoolPageNumber;
    }

    @SuppressWarnings("unused")
    public void setBufferPoolPageNumber(short bufferPoolPageNumber) {
        this.bufferPoolPageNumber = bufferPoolPageNumber;
    }

    public int getFrontSocketSoRcvbuf() {
        return frontSocketSoRcvbuf;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketSoRcvbuf(int frontSocketSoRcvbuf) {
        this.frontSocketSoRcvbuf = frontSocketSoRcvbuf;
    }

    public int getFrontSocketSoSndbuf() {
        return frontSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketSoSndbuf(int frontSocketSoSndbuf) {
        this.frontSocketSoSndbuf = frontSocketSoSndbuf;
    }

    public int getBackSocketSoRcvbuf() {
        return backSocketSoRcvbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoRcvbuf(int backSocketSoRcvbuf) {
        this.backSocketSoRcvbuf = backSocketSoRcvbuf;
    }

    public int getBackSocketSoSndbuf() {
        return backSocketSoSndbuf;
    }

    @SuppressWarnings("unused")
    public void setBackSocketSoSndbuf(int backSocketSoSndbuf) {
        this.backSocketSoSndbuf = backSocketSoSndbuf;
    }

    public int getFrontSocketNoDelay() {
        return frontSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setFrontSocketNoDelay(int frontSocketNoDelay) {
        if (frontSocketNoDelay >= 0 && frontSocketNoDelay <= 1) {
            this.frontSocketNoDelay = frontSocketNoDelay;
        } else {
            LOGGER.warn("frontSocketNoDelay " + frontSocketNoDelay + " in server.xml is not recognized ,use " + this.frontSocketNoDelay + " replaced");
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
            LOGGER.warn("backSocketNoDelay " + backSocketNoDelay + " in server.xml is not recognized ,use " + this.backSocketNoDelay + " replaced");
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
            LOGGER.warn("usingAIO " + usingAIO + " in server.xml is not recognized ,use " + this.usingAIO + " replaced");
        }
    }

    public int getServerNodeId() {
        return serverNodeId;
    }

    @SuppressWarnings("unused")
    public void setServerNodeId(int serverNodeId) {
        this.serverNodeId = serverNodeId;
    }

    public int getCheckTableConsistency() {
        return checkTableConsistency;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistency(int checkTableConsistency) {
        if (checkTableConsistency >= 0 && checkTableConsistency <= 1) {
            this.checkTableConsistency = checkTableConsistency;
        } else {
            LOGGER.warn("checkTableConsistency " + checkTableConsistency + " in server.xml is not recognized ,use " + this.checkTableConsistency + " replaced");
        }
    }

    public long getCheckTableConsistencyPeriod() {
        return checkTableConsistencyPeriod;
    }

    @SuppressWarnings("unused")
    public void setCheckTableConsistencyPeriod(long checkTableConsistencyPeriod) {
        this.checkTableConsistencyPeriod = checkTableConsistencyPeriod;
    }

    public int getNestLoopRowsSize() {
        return nestLoopRowsSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopRowsSize(int nestLoopRowsSize) {
        this.nestLoopRowsSize = nestLoopRowsSize;
    }

    public int getJoinQueueSize() {
        return joinQueueSize;
    }

    @SuppressWarnings("unused")
    public void setJoinQueueSize(int joinQueueSize) {
        this.joinQueueSize = joinQueueSize;
    }

    public int getMergeQueueSize() {
        return mergeQueueSize;
    }

    @SuppressWarnings("unused")
    public void setMergeQueueSize(int mergeQueueSize) {
        this.mergeQueueSize = mergeQueueSize;
    }

    public int getOtherMemSize() {
        return otherMemSize;
    }

    @SuppressWarnings("unused")
    public void setOtherMemSize(int otherMemSize) {
        this.otherMemSize = otherMemSize;
    }

    public int getOrderMemSize() {
        return orderMemSize;
    }

    @SuppressWarnings("unused")
    public void setOrderMemSize(int orderMemSize) {
        this.orderMemSize = orderMemSize;
    }

    public int getJoinMemSize() {
        return joinMemSize;
    }

    @SuppressWarnings("unused")
    public void setJoinMemSize(int joinMemSize) {
        this.joinMemSize = joinMemSize;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    @SuppressWarnings("unused")
    public void setMappedFileSize(int mappedFileSize) {
        this.mappedFileSize = mappedFileSize;
    }

    public int getNestLoopConnSize() {
        return nestLoopConnSize;
    }

    @SuppressWarnings("unused")
    public void setNestLoopConnSize(int nestLoopConnSize) {
        this.nestLoopConnSize = nestLoopConnSize;
    }

    public int getOrderByQueueSize() {
        return orderByQueueSize;
    }

    @SuppressWarnings("unused")
    public void setOrderByQueueSize(int orderByQueueSize) {
        this.orderByQueueSize = orderByQueueSize;
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
        } else {
            LOGGER.warn("useCostTimeStat " + useCostTimeStat + " in server.xml is not recognized ,use " + this.useCostTimeStat + " replaced");
        }
    }

    public int getMaxCostStatSize() {
        return maxCostStatSize;
    }
    @SuppressWarnings("unused")
    public void setMaxCostStatSize(int maxCostStatSize) {
        this.maxCostStatSize = maxCostStatSize;
    }

    public int getCostSamplePercent() {
        return costSamplePercent;
    }
    @SuppressWarnings("unused")
    public void setCostSamplePercent(int costSamplePercent) {
        this.costSamplePercent = costSamplePercent;
    }


    public int getUseThreadUsageStat() {
        return useThreadUsageStat;
    }

    @SuppressWarnings("unused")
    public void setUseThreadUsageStat(int useThreadUsageStat) {
        if (useThreadUsageStat >= 0 && useThreadUsageStat <= 1) {
            this.useThreadUsageStat = useThreadUsageStat;
        } else {
            LOGGER.warn("useThreadUsageStat " + useThreadUsageStat + " in server.xml is not recognized ,use " + this.useThreadUsageStat + " replaced");
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
            LOGGER.warn("usePerformanceMode " + usePerformanceMode + " in server.xml is not recognized ,use " + this.usePerformanceMode + " replaced");
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
        } else {
            LOGGER.warn("enableSlowLog " + enableSlowLog + " in server.xml is not recognized ,use " + this.enableSlowLog + " replaced");
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
        this.flushSlowLogPeriod = flushSlowLogPeriod;
    }

    public int getFlushSlowLogSize() {
        return flushSlowLogSize;
    }

    @SuppressWarnings("unused")
    public void setFlushSlowLogSize(int flushSlowLogSize) {
        this.flushSlowLogSize = flushSlowLogSize;
    }

    public int getSqlSlowTime() {
        return sqlSlowTime;
    }

    @SuppressWarnings("unused")
    public void setSqlSlowTime(int sqlSlowTime) {
        this.sqlSlowTime = sqlSlowTime;
    }


    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
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
                ", useZKSwitch=" + useZKSwitch +
                ", useThreadUsageStat=" + useThreadUsageStat +
                ", usePerformanceMode=" + usePerformanceMode +
                ", useCostTimeStat=" + useCostTimeStat +
                ", maxCostStatSize=" + maxCostStatSize +
                ", costSamplePercent=" + costSamplePercent +
                ", charset=" + charset +
                ", maxPacketSize=" + maxPacketSize +
                ", txIsolation=" + txIsolation +
                ", checkTableConsistency=" + checkTableConsistency +
                ", checkTableConsistencyPeriod=" + checkTableConsistencyPeriod +
                ", useGlobleTableCheck=" + useGlobleTableCheck +
                ", glableTableCheckPeriod=" + glableTableCheckPeriod +
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
                ", clusterHeartbeatUser=" + clusterHeartbeatUser +
                ", clusterHeartbeatPass=" + clusterHeartbeatPass +
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
                "]";
    }

    //tmp
    public int getUseOldMetaInit() {
        return useOldMetaInit;
    }

    public void setUseOldMetaInit(int useOldMetaInit) {
        this.useOldMetaInit = useOldMetaInit;
    }

    private int useOldMetaInit = 0;

}
