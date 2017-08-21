/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.config.model;

import io.mycat.config.Isolations;

import java.io.File;
import java.io.IOException;

/**
 * 系统基础配置项
 *
 * @author mycat
 */
public final class SystemConfig {

    public static final String SYS_HOME = "MYCAT_HOME";
    public static final String XA_COMMIT_DELAY = "COMMIT_DELAY";
    public static final String XA_PREPARE_DELAY = "PREPARE_DELAY";
    public static final String XA_ROLLBACK_DELAY = "ROLLBACK_DELAY";
    static final long DEFAULT_IDLE_TIMEOUT = 30 * 60 * 1000L;
    public static final int SEQUENCEHANDLER_MYSQLDB = 1;
    public static final int SEQUENCEHANDLER_LOCAL_TIME = 2;
    public static final int SEQUENCEHANDLER_ZK_DISTRIBUTED = 3;
    public static final int SEQUENCEHANDLER_ZK_GLOBAL_INCREMENT = 4;
    /*
     * 注意！！！ 目前mycat支持的MySQL版本，如果后续有新的MySQL版本,请添加到此数组， 对于MySQL的其他分支，
     * 比如MariaDB目前版本号已经到10.1.x，但是其驱动程序仍然兼容官方的MySQL,因此这里版本号只需要MySQL官方的版本号即可。
     */
    public static final String[] MySQLVersions = {"5.5", "5.6", "5.7"};

    private static final int DEFAULT_PORT = 8066;
    private static final int DEFAULT_MANAGER_PORT = 9066;
    private static final int DEFAULT_BACK_LOG_SIZE = 2048;
    private static final String DEFAULT_CHARSET = "utf8";
    private static final short DEFAULT_BUFFER_CHUNK_SIZE = 4096;
    private static final int DEFAULT_BUFFER_POOL_PAGE_SIZE = 512 * 1024 * 4;
    private static final int DEFAULT_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private final static String MEMORY_PAGE_SIZE = "1m";
    private final static String SPILLS_FILE_BUFFER_SIZE = "2K";
    private static final long DEFAULT_PROCESSOR_CHECK_PERIOD = 1000L;
    private static final long DEFAULT_XA_SESSION_CHECK_PERIOD = 1000L;
    private static final long DEFAULT_XA_LOG_CLEAN_PERIOD = 1000L;
    private static final long DEFAULT_DATANODE_IDLE_CHECK_PERIOD = 5 * 60 * 1000L;
    private static final long DEFAULT_DATANODE_HEARTBEAT_PERIOD = 10 * 1000L;
    private static final String DEFAULT_CLUSTER_HEARTBEAT_USER = "_HEARTBEAT_USER_";
    private static final String DEFAULT_CLUSTER_HEARTBEAT_PASS = "_HEARTBEAT_PASS_";
    private static final int DEFAULT_SQL_RECORD_COUNT = 10;
    private static final boolean DEFAULT_USE_ZK_SWITCH = true;
    private static final boolean DEFAULT_LOWER_CASE = true;
    private static final String DEFAULT_TRANSACTION_BASE_DIR = "txlogs";
    private static final String DEFAULT_TRANSACTION_BASE_NAME = "server-tx";
    private static final int DEFAULT_TRANSACTION_ROTATE_SIZE = 16;
    private static final long CHECKTABLECONSISTENCYPERIOD = 30 * 60 * 1000;
    // 全局表一致性检测任务，默认24小时调度一次
    private static final long DEFAULT_GLOBAL_TABLE_CHECK_PERIOD = 24 * 60 * 60 * 1000L;
    private static final int DEFAULT_MERGE_QUEUE_SIZE = 1024;
    private static final int DEFAULT_ORDERBY_QUEUE_SIZE = 1024;
    private static final int DEFAULT_JOIN_QUEUE_SIZE = 1024;
    private static final int DEFAULT_NESTLOOP_ROWS_SIZE = 2000;
    private static final int DEFAULT_NESTLOOP_CONN_SIZE = 4;
    private static final int DEFAULT_MAPPEDFILE_SIZE = 1024 * 1024 * 64;
    private static final boolean DEFAULT_USE_JOINSTRATEGY = false;

    private int frontSocketSoRcvbuf = 1024 * 1024;
    private int frontSocketSoSndbuf = 4 * 1024 * 1024;
    // mysql 5.6 net_buffer_length defaut 4M
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
    private int managerExecutor;
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
    //大结果集阈值，默认512kb
    private int maxResultSet = 512 * 1024;
    //大结果集拒绝策咯，bufferpool使用率阈值(0-100)，默认80%
    private int bufferUsagePercent = 80;
    //清理大结果集记录周期
    private long clearBigSqLResultSetMapMs = 10 * 60 * 1000;
    private int sequnceHandlerType = SEQUENCEHANDLER_LOCAL_TIME;
    private int usingAIO = 0;
    private int maxPacketSize = 16 * 1024 * 1024;
    private int serverNodeId = 1;
    private int useCompression = 0;
    private int useSqlStat = 1;

    // 是否使用HandshakeV10Packet来与client进行通讯, 1:是 , 0:否(使用HandshakePacket)
    // 使用HandshakeV10Packet为的是兼容高版本的jdbc驱动, 后期稳定下来考虑全部采用HandshakeV10Packet来通讯
    private int useHandshakeV10 = 0;
    private int checkTableConsistency = 0;
    private long checkTableConsistencyPeriod = CHECKTABLECONSISTENCYPERIOD;
    private int useGlobleTableCheck = 1;    // 全局表一致性检查开关
    private long glableTableCheckPeriod;

     /* 使用 Off Heap For Merge/Order/Group/Limit计算相关参数
     */
    /**
     * 是否启用Off Heap for Merge  1-启用，0-不启用
     */
    private int useOffHeapForMerge;
    /*
     *页大小,对应MemoryBlock的大小，单位为M
     */
    private String memoryPageSize;
    /*
     * DiskRowWriter写磁盘是临时写Buffer，单位为K
     */
    private String spillsFileBufferSize;

    /*
     * 排序时，内存不够时，将已经排序的结果集
     * 写入到临时目录
     */
    private String dataNodeSortedTempDir;
    /*
     * 该变量仅在Merge使用On Heap
     * 内存方式时起作用，如果使用Off Heap内存方式
     * 那么可以认为-Xmx就是系统预留内存。
     * 在On Heap上给系统预留的内存，
     * 主要供新小对象创建，JAVA简单数据结构使用
     * 以保证在On Heap上大结果集计算时情况，能快速响应其他
     * 连接操作。
     */
    private String XARecoveryLogBaseDir;
    private String XARecoveryLogBaseName;
    private String transactionLogBaseDir;
    private String transactionLogBaseName;
    private int transactionRatateSize;

    private int mergeQueueSize;
    private int orderByQueueSize;
    private int joinQueueSize;
    private int nestLoopRowsSize;
    private int nestLoopConnSize;
    private int mappedFileSize;
    /**
     * 是否启用zk切换
     */
    private boolean useZKSwitch = DEFAULT_USE_ZK_SWITCH;

    private boolean lowerCaseTableNames = DEFAULT_LOWER_CASE;

    // 是否使用JoinStrategy优化
    private boolean useJoinStrategy;

    public SystemConfig() {
        this.serverPort = DEFAULT_PORT;
        this.managerPort = DEFAULT_MANAGER_PORT;
        this.serverBacklog = DEFAULT_BACK_LOG_SIZE;
        this.charset = DEFAULT_CHARSET;
        this.processors = DEFAULT_PROCESSORS;
        this.bufferPoolPageSize = DEFAULT_BUFFER_POOL_PAGE_SIZE;
        this.bufferPoolChunkSize = DEFAULT_BUFFER_CHUNK_SIZE;
        // 大结果集时 需增大 network buffer pool pages.
        this.bufferPoolPageNumber = (short) (DEFAULT_PROCESSORS * 20);

        this.processorExecutor = (DEFAULT_PROCESSORS != 1) ? DEFAULT_PROCESSORS * 2 : 4;
        this.managerExecutor = 2;

        this.idleTimeout = DEFAULT_IDLE_TIMEOUT;
        this.processorCheckPeriod = DEFAULT_PROCESSOR_CHECK_PERIOD;
        this.xaSessionCheckPeriod = DEFAULT_XA_SESSION_CHECK_PERIOD;
        this.xaLogCleanPeriod = DEFAULT_XA_LOG_CLEAN_PERIOD;
        this.dataNodeIdleCheckPeriod = DEFAULT_DATANODE_IDLE_CHECK_PERIOD;
        this.dataNodeHeartbeatPeriod = DEFAULT_DATANODE_HEARTBEAT_PERIOD;
        this.clusterHeartbeatUser = DEFAULT_CLUSTER_HEARTBEAT_USER;
        this.clusterHeartbeatPass = DEFAULT_CLUSTER_HEARTBEAT_PASS;
        this.txIsolation = Isolations.REPEATED_READ;
        this.sqlRecordCount = DEFAULT_SQL_RECORD_COUNT;
        this.glableTableCheckPeriod = DEFAULT_GLOBAL_TABLE_CHECK_PERIOD;
        this.useOffHeapForMerge = 1;
        this.memoryPageSize = MEMORY_PAGE_SIZE;
        this.spillsFileBufferSize = SPILLS_FILE_BUFFER_SIZE;
        this.XARecoveryLogBaseDir = SystemConfig.getHomePath() + "/tmlogs/";
        this.XARecoveryLogBaseName = "tmlog";
        this.transactionLogBaseDir = SystemConfig.getHomePath() + File.separatorChar + DEFAULT_TRANSACTION_BASE_DIR;
        this.transactionLogBaseName = DEFAULT_TRANSACTION_BASE_NAME;
        this.transactionRatateSize = DEFAULT_TRANSACTION_ROTATE_SIZE;
        this.mergeQueueSize = DEFAULT_MERGE_QUEUE_SIZE;
        this.orderByQueueSize = DEFAULT_ORDERBY_QUEUE_SIZE;
        this.joinQueueSize = DEFAULT_JOIN_QUEUE_SIZE;
        this.nestLoopRowsSize = DEFAULT_NESTLOOP_ROWS_SIZE;
        this.nestLoopConnSize = DEFAULT_NESTLOOP_CONN_SIZE;
        this.mappedFileSize = DEFAULT_MAPPEDFILE_SIZE;
        this.useJoinStrategy = DEFAULT_USE_JOINSTRATEGY;
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

    public String getXARecoveryLogBaseDir() {
        return XARecoveryLogBaseDir;
    }

    @SuppressWarnings("unused")
    public void setXARecoveryLogBaseDir(String XARecoveryLogBaseDir) {
        this.XARecoveryLogBaseDir = XARecoveryLogBaseDir;
    }

    public String getXARecoveryLogBaseName() {
        return XARecoveryLogBaseName;
    }

    @SuppressWarnings("unused")
    public void setXARecoveryLogBaseName(String XARecoveryLogBaseName) {
        this.XARecoveryLogBaseName = XARecoveryLogBaseName;
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
        if (home != null
                && home.endsWith(File.pathSeparator)) {
            home = home.substring(0, home.length() - 1);
            System.setProperty(SystemConfig.SYS_HOME, home);
        }

        // MYCAT_HOME为空，默认尝试设置为当前目录或上级目录。BEN
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
                // 如出错，则忽略。
            }
        }

        return home;
    }

    // 是否使用SQL统计
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

    @SuppressWarnings("unused")
    public int getManagerExecutor() {
        return managerExecutor;
    }

    @SuppressWarnings("unused")
    public void setManagerExecutor(int managerExecutor) {
        this.managerExecutor = managerExecutor;
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

    public long getxaSessionCheckPeriod() {
        return xaSessionCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setxaSessionCheckPeriod(long xaSessionCheckPeriod) {
        this.xaSessionCheckPeriod = xaSessionCheckPeriod;
    }

    public long getxaLogCleanPeriod() {
        return xaLogCleanPeriod;
    }

    @SuppressWarnings("unused")
    public void setxaLogCleanPeriod(long xaLogCleanPeriod) {
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

    @Override
    public String toString() {
        return "SystemConfig [frontSocketSoRcvbuf="
                + frontSocketSoRcvbuf
                + ", frontSocketSoSndbuf=" + frontSocketSoSndbuf
                + ", backSocketSoRcvbuf=" + backSocketSoRcvbuf
                + ", backSocketSoSndbuf=" + backSocketSoSndbuf
                + ", frontSocketNoDelay=" + frontSocketNoDelay
                + ", backSocketNoDelay=" + backSocketNoDelay
                + ", bindIp=" + bindIp
                + ", serverPort=" + serverPort
                + ", managerPort=" + managerPort
                + ", charset=" + charset
                + ", processors=" + processors
                + ", processorExecutor=" + processorExecutor
                + ", managerExecutor=" + managerExecutor
                + ", idleTimeout=" + idleTimeout
                + ", sqlExecuteTimeout=" + sqlExecuteTimeout
                + ", showBinlogStatusTimeout=" + showBinlogStatusTimeout
                + ", processorCheckPeriod=" + processorCheckPeriod
                + ", dataNodeIdleCheckPeriod=" + dataNodeIdleCheckPeriod
                + ", dataNodeHeartbeatPeriod=" + dataNodeHeartbeatPeriod
                + ", xaSessionCheckPeriod=" + xaSessionCheckPeriod
                + ", xaLogCleanPeriod=" + xaLogCleanPeriod
                + ", transactionLogBaseDir=" + transactionLogBaseDir
                + ", transactionLogBaseName=" + transactionLogBaseName
                + ", clusterHeartbeatUser=" + clusterHeartbeatUser
                + ", clusterHeartbeatPass=" + clusterHeartbeatPass
                + ", txIsolation=" + txIsolation
                + ", sqlRecordCount=" + sqlRecordCount
                + ", bufferPoolPageSize=" + bufferPoolPageSize
                + ", bufferPoolChunkSize=" + bufferPoolChunkSize
                + ", bufferPoolPageNumber=" + bufferPoolPageNumber
                + ", maxResultSet=" + maxResultSet
                + ", bufferUsagePercent=" + bufferUsagePercent
                + ", clearBigSqLResultSetMapMs=" + clearBigSqLResultSetMapMs
                + ", sequnceHandlerType=" + sequnceHandlerType
                + ", usingAIO=" + usingAIO
                + ", maxPacketSize=" + maxPacketSize
                + ", serverNodeId=" + serverNodeId
                + ", dataNodeSortedTempDir=" + dataNodeSortedTempDir
                + "]";
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

    public int getUseHandshakeV10() {
        return useHandshakeV10;
    }

    @SuppressWarnings("unused")
    public void setUseHandshakeV10(int useHandshakeV10) {
        this.useHandshakeV10 = useHandshakeV10;
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

}
