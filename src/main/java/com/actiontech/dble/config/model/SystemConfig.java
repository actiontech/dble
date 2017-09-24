/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.config.Isolations;

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
    /*
     * the supported  protocol version of MySQL
     * For Other MySQL branch ,like MariaDB 10.1.x,
     * but its protocol is compatible with MySQL. So the versions array only contain official version here
     */
    public static final String[] MYSQL_VERSIONS = {"5.5", "5.6", "5.7"};

    private static final int DEFAULT_PORT = 8066;
    private static final int DEFAULT_MANAGER_PORT = 9066;
    private static final int DEFAULT_BACK_LOG_SIZE = 2048;
    private static final String DEFAULT_CHARSET = "utf8";
    private static final short DEFAULT_BUFFER_CHUNK_SIZE = 4096;
    private static final int DEFAULT_BUFFER_POOL_PAGE_SIZE = 512 * 1024 * 4;
    private static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final String MEMORY_PAGE_SIZE = "1m";
    private static final String SPILLS_FILE_BUFFER_SIZE = "2K";
    private static final long DEFAULT_PROCESSOR_CHECK_PERIOD = 1000L;
    private static final long DEFAULT_XA_SESSION_CHECK_PERIOD = 1000L;
    private static final long DEFAULT_XA_LOG_CLEAN_PERIOD = 1000L;
    private static final long DEFAULT_DATA_NODE_IDLE_CHECK_PERIOD = 5 * 60 * 1000L;
    private static final long DEFAULT_DATA_NODE_HEARTBEAT_PERIOD = 10 * 1000L;
    private static final String DEFAULT_CLUSTER_HEARTBEAT_USER = "_HEARTBEAT_USER_";
    private static final String DEFAULT_CLUSTER_HEARTBEAT_PASS = "_HEARTBEAT_PASS_";
    private static final int DEFAULT_SQL_RECORD_COUNT = 10;
    private static final boolean DEFAULT_USE_ZK_SWITCH = true;
    private static final boolean DEFAULT_LOWER_CASE = true;
    private static final String DEFAULT_TRANSACTION_BASE_DIR = "txlogs";
    private static final String DEFAULT_TRANSACTION_BASE_NAME = "server-tx";
    private static final int DEFAULT_TRANSACTION_ROTATE_SIZE = 16;
    private static final long CHECK_TABLE_CONSISTENCY_PERIOD = 30 * 60 * 1000;
    private static final long DEFAULT_GLOBAL_TABLE_CHECK_PERIOD = 24 * 60 * 60 * 1000L;
    private static final int DEFAULT_MERGE_QUEUE_SIZE = 1024;
    private static final int DEFAULT_ORDER_BY_QUEUE_SIZE = 1024;
    private static final int DEFAULT_JOIN_QUEUE_SIZE = 1024;
    private static final int DEFAULT_NEST_LOOP_ROWS_SIZE = 2000;
    private static final int DEFAULT_NEST_LOOP_CONN_SIZE = 4;
    private static final int DEFAULT_MAPPED_FILE_SIZE = 1024 * 1024 * 64;
    private static final boolean DEFAULT_USE_JOIN_STRATEGY = false;

    private int frontSocketSoRcvbuf = 1024 * 1024;
    private int frontSocketSoSndbuf = 4 * 1024 * 1024;
    // mysql 5.6 net_buffer_length default 4M
    private int backSocketSoRcvbuf = 4 * 1024 * 1024;
    private int backSocketSoSndbuf = 1024 * 1024;
    private int frontSocketNoDelay = 1; // 0=false
    private int backSocketNoDelay = 1; // 1=true
    private String bindIp = "0.0.0.0";
    private String fakeMySQLVersion = null;
    private int serverPort;
    private int managerPort;
    private int serverBacklog;
    private String charset;
    private int processors;
    private int processorExecutor;
    private long idleTimeout;
    // sql execute timeout (second)
    private long sqlExecuteTimeout = 300;
    private long showBinlogStatusTimeout = 60 * 1000;
    private long processorCheckPeriod;
    private long xaSessionCheckPeriod;
    private long xaLogCleanPeriod;
    private long dataNodeIdleCheckPeriod;
    private long dataNodeHeartbeatPeriod;
    private String clusterHeartbeatUser;
    private String clusterHeartbeatPass;
    private int txIsolation;
    private int sqlRecordCount;
    private int recordTxn = 0;
    // a page size
    private int bufferPoolPageSize;
    //minimum allocation unit
    private short bufferPoolChunkSize;
    // buffer pool page number
    private short bufferPoolPageNumber;
    //Threshold of big result ,default512kb
    private int maxResultSet = 512 * 1024;
    //Threshold of Usage Percent of buffer pool,if reached the Threshold,big result will be clean up,default 80%
    private int bufferUsagePercent = 80;
    //period of clear the big result
    private long clearBigSqLResultSetMapMs = 10 * 60 * 1000;
    private int sequnceHandlerType = SEQUENCE_HANDLER_LOCAL_TIME;
    private int usingAIO = 0;
    private int maxPacketSize = 16 * 1024 * 1024;
    private int serverNodeId = 1;
    private int useCompression = 0;
    private int useSqlStat = 1;

    private int checkTableConsistency = 0;
    private long checkTableConsistencyPeriod = CHECK_TABLE_CONSISTENCY_PERIOD;
    private int useGlobleTableCheck = 1;
    private long glableTableCheckPeriod;

    /**
     * Off Heap for Merge  1-enable,0-disable
     */
    private int useOffHeapForMerge;
    /*
     * memoryPageSize the unit is M
     */
    private String memoryPageSize;
    private String spillsFileBufferSize;

    /*
     * tmp dir for big result sorted
     */
    private String dataNodeSortedTempDir;

    private String xaRecoveryLogBaseDir;
    private String xaRecoveryLogBaseName;
    private String transactionLogBaseDir;
    private String transactionLogBaseName;
    private int transactionRatateSize;

    private int mergeQueueSize;
    private int orderByQueueSize;
    private int joinQueueSize;
    private int nestLoopRowsSize;
    private int nestLoopConnSize;
    private int mappedFileSize;
    private boolean useZKSwitch = DEFAULT_USE_ZK_SWITCH;

    private boolean lowerCaseTableNames = DEFAULT_LOWER_CASE;

    private boolean useJoinStrategy;

    public SystemConfig() {
        this.serverPort = DEFAULT_PORT;
        this.managerPort = DEFAULT_MANAGER_PORT;
        this.serverBacklog = DEFAULT_BACK_LOG_SIZE;
        this.charset = DEFAULT_CHARSET;
        this.processors = DEFAULT_PROCESSORS;
        this.bufferPoolPageSize = DEFAULT_BUFFER_POOL_PAGE_SIZE;
        this.bufferPoolChunkSize = DEFAULT_BUFFER_CHUNK_SIZE;
        // if always big result,need large network buffer pool pages.
        this.bufferPoolPageNumber = (short) (DEFAULT_PROCESSORS * 20);

        this.processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS * 2 : 4;

        this.idleTimeout = DEFAULT_IDLE_TIMEOUT;
        this.processorCheckPeriod = DEFAULT_PROCESSOR_CHECK_PERIOD;
        this.xaSessionCheckPeriod = DEFAULT_XA_SESSION_CHECK_PERIOD;
        this.xaLogCleanPeriod = DEFAULT_XA_LOG_CLEAN_PERIOD;
        this.dataNodeIdleCheckPeriod = DEFAULT_DATA_NODE_IDLE_CHECK_PERIOD;
        this.dataNodeHeartbeatPeriod = DEFAULT_DATA_NODE_HEARTBEAT_PERIOD;
        this.clusterHeartbeatUser = DEFAULT_CLUSTER_HEARTBEAT_USER;
        this.clusterHeartbeatPass = DEFAULT_CLUSTER_HEARTBEAT_PASS;
        this.txIsolation = Isolations.REPEATED_READ;
        this.sqlRecordCount = DEFAULT_SQL_RECORD_COUNT;
        this.glableTableCheckPeriod = DEFAULT_GLOBAL_TABLE_CHECK_PERIOD;
        this.useOffHeapForMerge = 1;
        this.memoryPageSize = MEMORY_PAGE_SIZE;
        this.spillsFileBufferSize = SPILLS_FILE_BUFFER_SIZE;
        this.xaRecoveryLogBaseDir = SystemConfig.getHomePath() + "/tmlogs/";
        this.xaRecoveryLogBaseName = "tmlog";
        this.transactionLogBaseDir = SystemConfig.getHomePath() + File.separatorChar + DEFAULT_TRANSACTION_BASE_DIR;
        this.transactionLogBaseName = DEFAULT_TRANSACTION_BASE_NAME;
        this.transactionRatateSize = DEFAULT_TRANSACTION_ROTATE_SIZE;
        this.mergeQueueSize = DEFAULT_MERGE_QUEUE_SIZE;
        this.orderByQueueSize = DEFAULT_ORDER_BY_QUEUE_SIZE;
        this.joinQueueSize = DEFAULT_JOIN_QUEUE_SIZE;
        this.nestLoopRowsSize = DEFAULT_NEST_LOOP_ROWS_SIZE;
        this.nestLoopConnSize = DEFAULT_NEST_LOOP_CONN_SIZE;
        this.mappedFileSize = DEFAULT_MAPPED_FILE_SIZE;
        this.useJoinStrategy = DEFAULT_USE_JOIN_STRATEGY;
        this.dataNodeSortedTempDir = SystemConfig.getHomePath() + "/sortDirs";
    }


    public String getDataNodeSortedTempDir() {
        return dataNodeSortedTempDir;
    }

    @SuppressWarnings("unused")
    public void setDataNodeSortedTempDir(String dataNodeSortedTempDir) {
        this.dataNodeSortedTempDir = dataNodeSortedTempDir;
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

    public int getUseOffHeapForMerge() {
        return useOffHeapForMerge;
    }

    @SuppressWarnings("unused")
    public void setUseOffHeapForMerge(int useOffHeapForMerge) {
        this.useOffHeapForMerge = useOffHeapForMerge;
    }

    public String getMemoryPageSize() {
        return memoryPageSize;
    }

    @SuppressWarnings("unused")
    public void setMemoryPageSize(String memoryPageSize) {
        this.memoryPageSize = memoryPageSize;
    }

    public String getSpillsFileBufferSize() {
        return spillsFileBufferSize;
    }

    @SuppressWarnings("unused")
    public void setSpillsFileBufferSize(String spillsFileBufferSize) {
        this.spillsFileBufferSize = spillsFileBufferSize;
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


    public boolean isLowerCaseTableNames() {
        return lowerCaseTableNames;
    }

    @SuppressWarnings("unused")
    public void setLowerCaseTableNames(boolean lowerCaseTableNames) {
        this.lowerCaseTableNames = lowerCaseTableNames;
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
        this.useGlobleTableCheck = useGlobleTableCheck;
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
        this.sequnceHandlerType = sequnceHandlerType;
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
        this.useSqlStat = useSqlStat;
    }

    public int getUseCompression() {
        return useCompression;
    }

    @SuppressWarnings("unused")
    public void setUseCompression(int useCompression) {
        this.useCompression = useCompression;
    }

    public String getCharset() {
        return charset;
    }

    @SuppressWarnings("unused")
    public void setCharset(String charset) {
        this.charset = charset;
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

    public int getProcessorExecutor() {
        return processorExecutor;
    }

    @SuppressWarnings("unused")
    public void setProcessorExecutor(int processorExecutor) {
        this.processorExecutor = processorExecutor;
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
        this.txIsolation = txIsolation;
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
        this.recordTxn = recordTxn;
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
        this.bufferUsagePercent = bufferUsagePercent;
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
        this.frontSocketNoDelay = frontSocketNoDelay;
    }

    public int getBackSocketNoDelay() {
        return backSocketNoDelay;
    }

    @SuppressWarnings("unused")
    public void setBackSocketNoDelay(int backSocketNoDelay) {
        this.backSocketNoDelay = backSocketNoDelay;
    }

    public int getUsingAIO() {
        return usingAIO;
    }

    @SuppressWarnings("unused")
    public void setUsingAIO(int usingAIO) {
        this.usingAIO = usingAIO;
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
        this.checkTableConsistency = checkTableConsistency;
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


    @Override
    public String toString() {
        return "SystemConfig [" +
                "  frontSocketSoRcvbuf=" + frontSocketSoRcvbuf +
                ", frontSocketSoSndbuf=" + frontSocketSoSndbuf +
                ", backSocketSoRcvbuf=" + backSocketSoRcvbuf +
                ", backSocketSoSndbuf=" + backSocketSoSndbuf +
                ", frontSocketNoDelay=" + frontSocketNoDelay +
                ", backSocketNoDelay=" + backSocketNoDelay +
                ", bindIp=" + bindIp +
                ", serverPort=" + serverPort +
                ", managerPort=" + managerPort +
                ", charset=" + charset +
                ", processors=" + processors +
                ", processorExecutor=" + processorExecutor +
                ", idleTimeout=" + idleTimeout +
                ", sqlExecuteTimeout=" + sqlExecuteTimeout +
                ", showBinlogStatusTimeout=" + showBinlogStatusTimeout +
                ", processorCheckPeriod=" + processorCheckPeriod +
                ", dataNodeIdleCheckPeriod=" + dataNodeIdleCheckPeriod +
                ", dataNodeHeartbeatPeriod=" + dataNodeHeartbeatPeriod +
                ", xaSessionCheckPeriod=" + xaSessionCheckPeriod +
                ", xaLogCleanPeriod=" + xaLogCleanPeriod +
                ", transactionLogBaseDir=" + transactionLogBaseDir +
                ", transactionLogBaseName=" + transactionLogBaseName +
                ", clusterHeartbeatUser=" + clusterHeartbeatUser +
                ", clusterHeartbeatPass=" + clusterHeartbeatPass +
                ", txIsolation=" + txIsolation +
                ", sqlRecordCount=" + sqlRecordCount +
                ", bufferPoolPageSize=" + bufferPoolPageSize +
                ", bufferPoolChunkSize=" + bufferPoolChunkSize +
                ", bufferPoolPageNumber=" + bufferPoolPageNumber +
                ", maxResultSet=" + maxResultSet +
                ", bufferUsagePercent=" + bufferUsagePercent +
                ", clearBigSqLResultSetMapMs=" + clearBigSqLResultSetMapMs +
                ", sequnceHandlerType=" + sequnceHandlerType +
                ", usingAIO=" + usingAIO +
                ", maxPacketSize=" + maxPacketSize +
                ", serverNodeId=" + serverNodeId +
                ", dataNodeSortedTempDir=" + dataNodeSortedTempDir +
                "]";
    }
}
