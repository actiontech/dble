/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.net.DbleSocketOptions;
import com.actiontech.dble.util.NetUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static com.actiontech.dble.config.model.db.PoolConfig.DEFAULT_IDLE_TIMEOUT;


public final class SystemConfig {
    private final ProblemReporter problemReporter = StartProblemReporter.getInstance();
    private static final SystemConfig INSTANCE = new SystemConfig();
    public static final int FLOW_CONTROL_LOW_LEVEL = 256 * 1024;
    public static final int FLOW_CONTROL_HIGH_LEVEL = 4096 * 1024;

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
    // CHECKSTYLE:OFF
    private int NIOFrontRW = Runtime.getRuntime().availableProcessors();
    private int NIOBackendRW = NIOFrontRW;
    // CHECKSTYLE:ON
    private int frontWorker = (NIOFrontRW != 1) ? NIOFrontRW : 2;
    private int managerFrontWorker = (NIOFrontRW > 2) ? NIOFrontRW / 2 : 2;
    private int backendWorker = (NIOFrontRW != 1) ? NIOFrontRW : 2;
    private int complexQueryWorker = frontWorker > 8 ? 8 : frontWorker;
    private int writeToBackendWorker = (NIOFrontRW != 1) ? NIOFrontRW : 2;
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
    private long heartbeatSqlExecuteTimeout = 10;
    // connection will force close if received close packet but haven't been closed after closeTimeout milliseconds.
    // set the value too big is not a good idea.
    private long closeTimeout = 100;

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
    private long xaIdCheckPeriod = 300; // s

    //use JoinStrategy
    private boolean useJoinStrategy = false;
    private int nestLoopRowsSize = 2000;
    private int nestLoopConnSize = 4;

    // join Optimizer
    private boolean useNewJoinOptimizer = false;
    private int joinStrategyType = -1;

    //query memory used for per session,unit is M
    private int otherMemSize = 4;
    private int orderMemSize = 4;
    private int joinMemSize = 4;

    // off Heap unit:bytes
    // a page size
    private int bufferPoolPageSize = 1024 * 1024 * 2;
    //minimum allocation unit
    private short bufferPoolChunkSize = 4096;
    //used for cursor temp result
    private boolean enableCursor = false;
    private int maxHeapTableSize = 4096;
    private Integer heapTableBufferChunkSize = null;
    // buffer pool page number
    private short bufferPoolPageNumber = (short) (Platform.getMaxDirectMemory() * 0.8 / bufferPoolPageSize);
    private boolean useDefaultPageNumber = true;
    private int mappedFileSize = 1024 * 1024 * 64;

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
    private int slowQueueOverflowPolicy = 1;

    //general log
    private int enableGeneralLog = 0;
    private String generalLogFile = "general/general.log";
    private int generalLogFileSize = 16; //mb
    private int generalLogQueueSize = 4096;

    //sqldump log
    private String enableSqlDumpLog = null;
    private String sqlDumpLogBasePath = null;
    private String sqlDumpLogFileName = null;
    private String sqlDumpLogCompressFilePattern = null;
    private String sqlDumpLogOnStartupRotate = null;
    private String sqlDumpLogSizeBasedRotate = null;
    private String sqlDumpLogTimeBasedRotate = null;
    private String sqlDumpLogDeleteFileAge = null;
    private String sqlDumpLogCompressFilePath = null;

    //alert switch
    private int enableAlert = 1;
    //load data
    private int maxRowSizeToFile = 100000;
    private int enableBatchLoadData = 0;
    private int maxCharsPerColumn = 65535; // 128k,65535 chars

    private boolean enableFlowControl = false;
    private int flowControlHighLevel = FLOW_CONTROL_HIGH_LEVEL;
    private int flowControlLowLevel = FLOW_CONTROL_LOW_LEVEL;
    private boolean useOuterHa = true;

    private String traceEndPoint = null;
    private String traceSamplerType = null;
    private String traceSamplerParam = null;

    private String fakeMySQLVersion = "5.7.21";
    private MysqlVersion mysqlVersion;
    private int enableRoutePenetration = 0;
    private String routePenetrationRules = "";

    private int enableStatistic = 0;
    private int enableStatisticAnalysis = 0;
    private int associateTablesByEntryByUserTableSize = 1024;
    private int frontendByBackendByEntryByUserTableSize = 1024;
    private int tableByUserByEntryTableSize = 1024;
    private int statisticQueueSize = 4096;

    private int enableSessionActiveRatioStat = 1;
    private int enableConnectionAssociateThread = 1;

    // sampling
    private int samplingRate = 100;
    private int sqlLogTableSize = 1024;

    //Threshold of big result ,default512kb
    private int maxResultSet = 512 * 1024;

    //use inSubQueryTransformToJoin
    private boolean inSubQueryTransformToJoin = false;

    //use groupConcatMaxLen
    private int groupConcatMaxLen = 1024;

    // For rwSplitUser, Implement stickiness for read and write instances, the default value is 1000ms
    private long rwStickyTime = 1000;

    private String district = null;
    private String dataCenter = null;


    private boolean closeHeartBeatRecord = false;

    private String serverCertificateKeyStoreUrl = null;
    private String serverCertificateKeyStorePwd = null;
    private String trustCertificateKeyStoreUrl = null;
    private String trustCertificateKeyStorePwd = null;

    //guomi
    private String gmsslBothPfx = null;
    private String gmsslBothPfxPwd = null;
    private String gmsslRcaPem = null;
    private String gmsslOcaPem = null;
    private boolean supportSSL = false;

    private int enableAsyncRelease = 1;
    //unit: ms
    private long releaseTimeout = 10L;

    private int enableMemoryBufferMonitor = 0;
    private int enableMemoryBufferMonitorRecordPool = 1;

    //tcp
    private int tcpKeepIdle = DbleSocketOptions.getInstance().getTcpKeepIdle();
    private int tcpKeepInterval = DbleSocketOptions.getInstance().getTcpKeepInterval();
    private int tcpKeepCount = DbleSocketOptions.getInstance().getTcpKeepCount();


    //maximum number of rows in select result set in multi-table update
    private long queryForUpdateMaxRowsSize = 20000;

    public int getEnableAsyncRelease() {
        return enableAsyncRelease;
    }

    public void setEnableAsyncRelease(int enableAsyncRelease) {
        this.enableAsyncRelease = enableAsyncRelease;
    }

    public long getReleaseTimeout() {
        return releaseTimeout;
    }

    public void setReleaseTimeout(long releaseTimeout) {
        this.releaseTimeout = releaseTimeout;
    }

    public String getServerCertificateKeyStoreUrl() {
        return serverCertificateKeyStoreUrl;
    }

    @SuppressWarnings("unused")
    public void setServerCertificateKeyStoreUrl(String serverCertificateKeyStoreUrl) {
        if (serverCertificateKeyStoreUrl != null) {
            this.serverCertificateKeyStoreUrl = serverCertificateKeyStoreUrl;
        }
    }

    public String getServerCertificateKeyStorePwd() {
        return serverCertificateKeyStorePwd;
    }

    @SuppressWarnings("unused")
    public void setServerCertificateKeyStorePwd(String serverCertificateKeyStorePwd) {
        if (serverCertificateKeyStorePwd != null) {
            this.serverCertificateKeyStorePwd = serverCertificateKeyStorePwd;
        }
    }

    public String getTrustCertificateKeyStoreUrl() {
        return trustCertificateKeyStoreUrl;
    }

    @SuppressWarnings("unused")
    public void setTrustCertificateKeyStoreUrl(String trustCertificateKeyStoreUrl) {
        if (trustCertificateKeyStoreUrl != null) {
            this.trustCertificateKeyStoreUrl = trustCertificateKeyStoreUrl;
        }
    }

    public String getTrustCertificateKeyStorePwd() {
        return trustCertificateKeyStorePwd;
    }

    @SuppressWarnings("unused")
    public void setTrustCertificateKeyStorePwd(String trustCertificateKeyStorePwd) {
        if (trustCertificateKeyStorePwd != null) {
            this.trustCertificateKeyStorePwd = trustCertificateKeyStorePwd;
        }
    }

    public String getGmsslBothPfx() {
        return gmsslBothPfx;
    }

    @SuppressWarnings("unused")
    public void setGmsslBothPfx(String gmsslBothPfx) {
        this.gmsslBothPfx = gmsslBothPfx;
    }

    public String getGmsslBothPfxPwd() {
        return gmsslBothPfxPwd;
    }

    @SuppressWarnings("unused")
    public void setGmsslBothPfxPwd(String gmsslBothPfxPwd) {
        this.gmsslBothPfxPwd = gmsslBothPfxPwd;
    }

    public String getGmsslRcaPem() {
        return gmsslRcaPem;
    }

    @SuppressWarnings("unused")
    public void setGmsslRcaPem(String gmsslRcaPem) {
        this.gmsslRcaPem = gmsslRcaPem;
    }

    public String getGmsslOcaPem() {
        return gmsslOcaPem;
    }

    @SuppressWarnings("unused")
    public void setGmsslOcaPem(String gmsslOcaPem) {
        this.gmsslOcaPem = gmsslOcaPem;
    }


    public int getSamplingRate() {
        return samplingRate;
    }

    @SuppressWarnings("unused")
    public void setSamplingRate(int samplingRate) {
        if (samplingRate >= 0 && samplingRate <= 100) {
            this.samplingRate = samplingRate;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "samplingRate", samplingRate, this.samplingRate));
        }
    }

    public int getSqlLogTableSize() {
        return sqlLogTableSize;
    }

    @SuppressWarnings("unused")
    public void setSqlLogTableSize(int sqlLogTableSize) {
        if (sqlLogTableSize > 0) {
            this.sqlLogTableSize = sqlLogTableSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "sqlLogTableSize", sqlLogTableSize, this.sqlLogTableSize));
        }
    }

    public int getEnableSessionActiveRatioStat() {
        return enableSessionActiveRatioStat;
    }

    @SuppressWarnings("unused")
    public void setEnableSessionActiveRatioStat(int enableSessionActiveRatioStat) {
        if (enableSessionActiveRatioStat >= 0 && enableSessionActiveRatioStat <= 1) {
            this.enableSessionActiveRatioStat = enableSessionActiveRatioStat;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableFrontActiveRatioStat", enableSessionActiveRatioStat, this.enableSessionActiveRatioStat));
        }
    }

    public int getEnableConnectionAssociateThread() {
        return enableConnectionAssociateThread;
    }

    @SuppressWarnings("unused")
    public void setEnableConnectionAssociateThread(int enableConnectionAssociateThread) {
        if (enableConnectionAssociateThread >= 0 && enableConnectionAssociateThread <= 1) {
            this.enableConnectionAssociateThread = enableConnectionAssociateThread;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableConnectionAssociateThread", enableConnectionAssociateThread, this.enableConnectionAssociateThread));
        }
    }

    public int getEnableStatistic() {
        return enableStatistic;
    }

    @SuppressWarnings("unused")
    public void setEnableStatistic(int enableStatistic) {
        if (enableStatistic >= 0 && enableStatistic <= 1) {
            this.enableStatistic = enableStatistic;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableStatistic", enableStatistic, this.enableStatistic));
        }
    }

    public int getEnableStatisticAnalysis() {
        return enableStatisticAnalysis;
    }

    @SuppressWarnings("unused")
    public void setEnableStatisticAnalysis(int enableStatisticAnalysis) {
        if (enableStatisticAnalysis >= 0 && enableStatisticAnalysis <= 1) {
            this.enableStatisticAnalysis = enableStatisticAnalysis;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableStatisticAnalysis", enableStatisticAnalysis, this.enableStatisticAnalysis));
        }

    }

    public int getAssociateTablesByEntryByUserTableSize() {
        return associateTablesByEntryByUserTableSize;
    }

    @SuppressWarnings("unused")
    public void setAssociateTablesByEntryByUserTableSize(int associateTablesByEntryByUserTableSize) {
        if (associateTablesByEntryByUserTableSize < 1) {
            problemReporter.warn(String.format(WARNING_FORMAT, "associateTablesByEntryByUserTableSize", associateTablesByEntryByUserTableSize, this.associateTablesByEntryByUserTableSize));
        } else {
            this.associateTablesByEntryByUserTableSize = associateTablesByEntryByUserTableSize;
        }
    }

    public int getFrontendByBackendByEntryByUserTableSize() {
        return frontendByBackendByEntryByUserTableSize;
    }

    @SuppressWarnings("unused")
    public void setFrontendByBackendByEntryByUserTableSize(int frontendByBackendByEntryByUserTableSize) {
        if (frontendByBackendByEntryByUserTableSize < 1) {
            problemReporter.warn(String.format(WARNING_FORMAT, "frontendByBackendByEntryByUserTableSize", frontendByBackendByEntryByUserTableSize, this.frontendByBackendByEntryByUserTableSize));
        } else {
            this.frontendByBackendByEntryByUserTableSize = frontendByBackendByEntryByUserTableSize;
        }
    }

    public int getTableByUserByEntryTableSize() {
        return tableByUserByEntryTableSize;
    }

    @SuppressWarnings("unused")
    public void setTableByUserByEntryTableSize(int tableByUserByEntryTableSize) {
        if (tableByUserByEntryTableSize < 1) {
            problemReporter.warn(String.format(WARNING_FORMAT, "tableByUserByEntryTableSize", tableByUserByEntryTableSize, this.tableByUserByEntryTableSize));
        } else {
            this.tableByUserByEntryTableSize = tableByUserByEntryTableSize;
        }
    }

    public int getStatisticQueueSize() {
        return statisticQueueSize;
    }

    @SuppressWarnings("unused")
    public void setStatisticQueueSize(int statisticQueueSize) {
        if (statisticQueueSize < 1 || Integer.bitCount(statisticQueueSize) != 1) {
            problemReporter.warn("Property [ statisticQueueSize ] '" + statisticQueueSize + "' in bootstrap.cnf is illegal, size must not be less than 1 and must be a power of 2, you may need use the default value " + this.statisticQueueSize + " replaced");
        } else {
            this.statisticQueueSize = statisticQueueSize;
        }
    }

    public int getEnableGeneralLog() {
        return enableGeneralLog;
    }

    @SuppressWarnings("unused")
    public void setEnableGeneralLog(int enableGeneralLog) {
        if (enableGeneralLog >= 0 && enableGeneralLog <= 1) {
            this.enableGeneralLog = enableGeneralLog;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableGeneralLog", enableGeneralLog, this.enableGeneralLog));
        }
    }

    public String getGeneralLogFile() {
        return generalLogFile;
    }

    @SuppressWarnings("unused")
    public void setGeneralLogFile(String generalLogFile) {
        this.generalLogFile = generalLogFile;
    }

    public int getGeneralLogFileSize() {
        return generalLogFileSize;
    }

    @SuppressWarnings("unused")
    public void setGeneralLogFileSize(int generalLogFileSize) {
        if (generalLogFileSize > 0) {
            this.generalLogFileSize = generalLogFileSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "generalLogFileSize", generalLogFileSize, this.generalLogFileSize));
        }
    }

    public int getGeneralLogQueueSize() {
        return generalLogQueueSize;
    }

    @SuppressWarnings("unused")
    public void setGeneralLogQueueSize(int generalLogQueueSize) {
        if (generalLogQueueSize < 1 || Integer.bitCount(generalLogQueueSize) != 1) {
            problemReporter.warn("Property [ generalLogQueueSize ] '" + generalLogQueueSize + "' in bootstrap.cnf is illegal, size must not be less than 1 and must be a power of 2, you may need use the default value " + this.generalLogQueueSize + " replaced");
        } else {
            this.generalLogQueueSize = generalLogQueueSize;
        }

    }

    public String getEnableSqlDumpLog() {
        return enableSqlDumpLog;
    }

    @SuppressWarnings("unused")
    public void setEnableSqlDumpLog(String enableSqlDumpLog) {
        this.enableSqlDumpLog = enableSqlDumpLog;
    }

    public String getSqlDumpLogBasePath() {
        return sqlDumpLogBasePath;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogBasePath(String sqlDumpLogBasePath) {
        this.sqlDumpLogBasePath = sqlDumpLogBasePath;
    }

    public String getSqlDumpLogFileName() {
        return sqlDumpLogFileName;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogFileName(String sqlDumpLogFileName) {
        this.sqlDumpLogFileName = sqlDumpLogFileName;
    }

    public String getSqlDumpLogCompressFilePattern() {
        return sqlDumpLogCompressFilePattern;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogCompressFilePattern(String sqlDumpLogCompressFilePattern) {
        this.sqlDumpLogCompressFilePattern = sqlDumpLogCompressFilePattern;
    }

    public String getSqlDumpLogCompressFilePath() {
        return sqlDumpLogCompressFilePath;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogCompressFilePath(String sqlDumpLogCompressFilePath) {
        this.sqlDumpLogCompressFilePath = sqlDumpLogCompressFilePath;
    }

    public String getSqlDumpLogOnStartupRotate() {
        return sqlDumpLogOnStartupRotate;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogOnStartupRotate(String sqlDumpLogOnStartupRotate) {
        this.sqlDumpLogOnStartupRotate = sqlDumpLogOnStartupRotate;
    }

    public String getSqlDumpLogSizeBasedRotate() {
        return sqlDumpLogSizeBasedRotate;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogSizeBasedRotate(String sqlDumpLogSizeBasedRotate) {
        if (!StringUtil.isBlank(sqlDumpLogSizeBasedRotate)) {
            this.sqlDumpLogSizeBasedRotate = sqlDumpLogSizeBasedRotate;
        }
    }

    public String getSqlDumpLogTimeBasedRotate() {
        return sqlDumpLogTimeBasedRotate;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogTimeBasedRotate(String sqlDumpLogTimeBasedRotate) {
        this.sqlDumpLogTimeBasedRotate = sqlDumpLogTimeBasedRotate;
    }

    public String getSqlDumpLogDeleteFileAge() {
        return sqlDumpLogDeleteFileAge;
    }

    @SuppressWarnings("unused")
    public void setSqlDumpLogDeleteFileAge(String sqlDumpLogDeleteFileAge) {
        this.sqlDumpLogDeleteFileAge = sqlDumpLogDeleteFileAge;
    }

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


    public boolean isUseNewJoinOptimizer() {
        return useNewJoinOptimizer;
    }

    public void setUseNewJoinOptimizer(boolean useNewJoinOptimizer) {
        this.useNewJoinOptimizer = useNewJoinOptimizer;
    }

    public int getJoinStrategyType() {
        return joinStrategyType;
    }

    public void setJoinStrategyType(int joinStrategyType) {
        if (joinStrategyType < -1 || joinStrategyType > 2) {
            problemReporter.warn("Property [ joinStrategyType ] '" + joinStrategyType + "' in bootstrap.cnf is illegal, size must not be less than -1 and not be greater than 2, you may need use the default value " + this.joinStrategyType + " replaced");
        } else {
            this.joinStrategyType = joinStrategyType;
        }
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

    @SuppressWarnings("unused")
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

    public int getNIOFrontRW() {
        return NIOFrontRW;
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("unused")
    public void setNIOFrontRW(int NIOFrontRW) {
        if (NIOFrontRW > 0) {
            this.NIOFrontRW = NIOFrontRW;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("NIOFrontRW");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "NIOFrontRW", NIOFrontRW, this.NIOFrontRW));
        }
    }

    public int getNIOBackendRW() {
        return NIOBackendRW;
    }

    @SuppressWarnings("unused")
    public void setNIOBackendRW(int NIOBackendRW) {
        if (NIOBackendRW > 0) {
            this.NIOBackendRW = NIOBackendRW;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("NIOBackendRW");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "NIOBackendRW", NIOBackendRW, this.NIOBackendRW));
        }
    }
    // CHECKSTYLE:ON

    public int getFrontWorker() {
        return frontWorker;
    }

    @SuppressWarnings("unused")
    public void setFrontWorker(int frontWorker) {
        if (frontWorker > 0) {
            this.frontWorker = frontWorker;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("frontWorker");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "frontWorker", frontWorker, this.frontWorker));
        }
    }

    public int getManagerFrontWorker() {
        return managerFrontWorker;
    }

    @SuppressWarnings("unused")
    public void setManagerFrontWorker(int managerFrontWorker) {
        if (managerFrontWorker > 0) {
            this.managerFrontWorker = managerFrontWorker;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("managerFrontWorker");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "managerFrontWorker", managerFrontWorker, this.managerFrontWorker));
        }
    }

    public int getBackendWorker() {
        return backendWorker;
    }

    @SuppressWarnings("unused")
    public void setBackendWorker(int backendWorker) {
        if (backendWorker > 0) {
            this.backendWorker = backendWorker;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("backendWorker");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "backendWorker", backendWorker, this.backendWorker));
        }
    }

    public int getComplexQueryWorker() {
        return complexQueryWorker;
    }

    @SuppressWarnings("unused")
    public void setComplexQueryWorker(int complexQueryWorker) {
        if (complexQueryWorker > 0) {
            this.complexQueryWorker = complexQueryWorker;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("complexQueryWorker");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "complexQueryWorker", complexQueryWorker, this.complexQueryWorker));
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

    public long getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
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

    public long getXaIdCheckPeriod() {
        return xaIdCheckPeriod;
    }

    @SuppressWarnings("unused")
    public void setXaIdCheckPeriod(long xaIdCheckPeriod) {
        if (xaIdCheckPeriod <= 0) xaIdCheckPeriod = -1;
        this.xaIdCheckPeriod = xaIdCheckPeriod;
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


    public long getHeartbeatSqlExecuteTimeout() {
        return heartbeatSqlExecuteTimeout;
    }

    public void setHeartbeatSqlExecuteTimeout(long heartbeatSqlExecuteTimeout) {
        this.heartbeatSqlExecuteTimeout = heartbeatSqlExecuteTimeout;
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


    public int getWriteToBackendWorker() {
        return writeToBackendWorker;
    }

    @SuppressWarnings("unused")
    public void setWriteToBackendWorker(int writeToBackendWorker) {
        if (writeToBackendWorker > 0) {
            this.writeToBackendWorker = writeToBackendWorker;
        } else {
            String message = ParameterMapping.getErrorCompatibleMessage("writeToBackendWorker");
            problemReporter.warn(message + String.format(WARNING_FORMAT, "writeToBackendWorker", writeToBackendWorker, this.writeToBackendWorker));
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

    public int getFlowControlHighLevel() {
        return flowControlHighLevel;
    }

    @SuppressWarnings("unused")
    public void setFlowControlHighLevel(int flowControlHighLevel) {
        if (flowControlHighLevel >= 0) {
            this.flowControlHighLevel = flowControlHighLevel;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "flowControlHighLevel", flowControlHighLevel, this.flowControlHighLevel));
        }
    }

    public int getFlowControlLowLevel() {
        return flowControlLowLevel;
    }

    @SuppressWarnings("unused")
    public void setFlowControlLowLevel(int flowControlLowLevel) {
        if (flowControlLowLevel >= 0) {
            this.flowControlLowLevel = flowControlLowLevel;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "flowControlLowLevel", flowControlLowLevel, this.flowControlLowLevel));
        }
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
    public void setFakeMySQLVersion(String version) {
        this.fakeMySQLVersion = version;
        if (this.fakeMySQLVersion == null) return;
        this.mysqlVersion = VersionUtil.parseVersion(this.fakeMySQLVersion);
    }

    public MysqlVersion getMysqlVersion() {
        if (this.mysqlVersion == null && this.fakeMySQLVersion != null) {
            this.mysqlVersion = VersionUtil.parseVersion(this.fakeMySQLVersion);
        }
        return mysqlVersion;
    }

    public String getTraceEndPoint() {
        return traceEndPoint;
    }

    @SuppressWarnings("unused")
    public void setTraceEndPoint(String traceEndPoint) {
        this.traceEndPoint = traceEndPoint;
    }


    public String getTraceSamplerType() {
        return traceSamplerType;
    }

    @SuppressWarnings("unused")
    public void setTraceSamplerType(String traceSamplerType) {
        this.traceSamplerType = traceSamplerType;
    }

    public String getTraceSamplerParam() {
        return traceSamplerParam;
    }

    @SuppressWarnings("unused")
    public void setTraceSamplerParam(String traceSamplerParam) {
        this.traceSamplerParam = traceSamplerParam;
    }

    public int getMaxHeapTableSize() {
        return maxHeapTableSize;
    }

    @SuppressWarnings("unused")
    public void setMaxHeapTableSize(int maxHeapTableSize) {
        if (maxHeapTableSize >= 0) {
            this.maxHeapTableSize = maxHeapTableSize;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "maxHeapTableSize", maxHeapTableSize, this.maxHeapTableSize));
        }
    }

    public boolean isEnableCursor() {
        return enableCursor;
    }

    @SuppressWarnings("unused")
    public void setEnableCursor(boolean enableCursor) {
        this.enableCursor = enableCursor;
    }

    public Integer getHeapTableBufferChunkSize() {
        return heapTableBufferChunkSize;
    }

    public void setHeapTableBufferChunkSize(Integer heapTableBufferChunkSize) {
        this.heapTableBufferChunkSize = heapTableBufferChunkSize;
    }

    public int getEnableBatchLoadData() {
        return enableBatchLoadData;
    }

    @SuppressWarnings("unused")
    public void setEnableBatchLoadData(int enableBatchLoadData) {
        if (enableBatchLoadData >= 0 && enableBatchLoadData <= 1) {
            this.enableBatchLoadData = enableBatchLoadData;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableBatchLoadData", enableBatchLoadData, this.enableBatchLoadData));
        }
    }

    public boolean isInSubQueryTransformToJoin() {
        return inSubQueryTransformToJoin;
    }

    @SuppressWarnings("unused")
    public void setInSubQueryTransformToJoin(boolean inSubQueryTransformToJoin) {
        this.inSubQueryTransformToJoin = inSubQueryTransformToJoin;
    }

    public int getGroupConcatMaxLen() {
        return groupConcatMaxLen;
    }

    @SuppressWarnings("unused")
    public void setGroupConcatMaxLen(int maxLen) {
        if (maxLen >= 0) {
            this.groupConcatMaxLen = maxLen;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "groupConcatMaxLen", maxLen, this.groupConcatMaxLen));
        }
    }

    public long getRwStickyTime() {
        return rwStickyTime;
    }

    @SuppressWarnings("unused")
    public void setRwStickyTime(long rwStickyTime) {
        if (rwStickyTime >= 0) {
            this.rwStickyTime = rwStickyTime;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "rwStickyTime", rwStickyTime, this.rwStickyTime));
        }
    }


    public int isEnableRoutePenetration() {
        return enableRoutePenetration;
    }

    @SuppressWarnings("unused")
    public void setEnableRoutePenetration(int enableRoutePenetrationTmp) {
        if (enableRoutePenetrationTmp >= 0 && enableRoutePenetrationTmp <= 1) {
            this.enableRoutePenetration = enableRoutePenetrationTmp;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableRoutePenetration", enableRoutePenetrationTmp, this.enableRoutePenetration));
        }
    }

    public String getRoutePenetrationRules() {
        return routePenetrationRules;
    }

    @SuppressWarnings("unused")
    public void setRoutePenetrationRules(String sqlPenetrationRegexesTmp) {
        routePenetrationRules = sqlPenetrationRegexesTmp;
    }

    public String getDistrict() {
        return district;
    }

    @SuppressWarnings("unused")
    public void setDistrict(String district) throws UnsupportedEncodingException {
        checkChineseProperty(district, "district");
        this.district = district;
    }

    public String getDataCenter() {
        return dataCenter;
    }

    @SuppressWarnings("unused")
    public void setDataCenter(String dataCenter) {
        checkChineseProperty(dataCenter, "dataCenter");
        this.dataCenter = dataCenter;
    }

    public boolean isSupportSSL() {
        return supportSSL;
    }

    @SuppressWarnings("unused")
    public void setSupportSSL(boolean supportSSL) {
        this.supportSSL = supportSSL;
    }


    public int getEnableMemoryBufferMonitor() {
        return enableMemoryBufferMonitor;
    }

    public void setEnableMemoryBufferMonitor(int enableBufferMonitorTmp) {
        if (enableBufferMonitorTmp >= 0 && enableBufferMonitorTmp <= 1) {
            this.enableMemoryBufferMonitor = enableBufferMonitorTmp;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableMemoryBufferMonitor", enableBufferMonitorTmp, this.enableMemoryBufferMonitor));
        }
    }

    public int getEnableMemoryBufferMonitorRecordPool() {
        return enableMemoryBufferMonitorRecordPool;
    }

    public void setEnableMemoryBufferMonitorRecordPool(int enableBufferMonitorRecordPoolTmp) {
        if (enableBufferMonitorRecordPoolTmp >= 0 && enableBufferMonitorRecordPoolTmp <= 1) {
            this.enableMemoryBufferMonitorRecordPool = enableBufferMonitorRecordPoolTmp;
        } else if (this.problemReporter != null) {
            problemReporter.warn(String.format(WARNING_FORMAT, "enableMemoryBufferMonitorRecordPool", enableBufferMonitorRecordPoolTmp, this.enableMemoryBufferMonitorRecordPool));
        }
    }


    public long getQueryForUpdateMaxRowsSize() {
        return queryForUpdateMaxRowsSize;
    }

    public void setQueryForUpdateMaxRowsSize(long queryForUpdateMaxRowsSize) {
        if (queryForUpdateMaxRowsSize >= 0) {
            this.queryForUpdateMaxRowsSize = queryForUpdateMaxRowsSize;
        }
    }

    public int getSlowQueueOverflowPolicy() {
        return slowQueueOverflowPolicy;
    }

    @SuppressWarnings("unused")
    public void setSlowQueueOverflowPolicy(int slowQueueOverflowPolicy) {
        if (slowQueueOverflowPolicy == 1 || slowQueueOverflowPolicy == 2) {
            this.slowQueueOverflowPolicy = slowQueueOverflowPolicy;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "slowQueueOverflowPolicy", slowQueueOverflowPolicy, this.slowQueueOverflowPolicy));
        }
    }

    public int getTcpKeepIdle() {
        return tcpKeepIdle;
    }

    public void setTcpKeepIdle(int tcpKeepIdle) {
        if (tcpKeepIdle > 0) {
            this.tcpKeepIdle = tcpKeepIdle;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepIdle", tcpKeepIdle, this.tcpKeepIdle));
        }
    }

    public int getTcpKeepInterval() {
        return tcpKeepInterval;
    }

    public void setTcpKeepInterval(int tcpKeepInterval) {
        if (tcpKeepInterval > 0) {
            this.tcpKeepInterval = tcpKeepInterval;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepInterval", tcpKeepInterval, this.tcpKeepInterval));
        }
    }

    public int getTcpKeepCount() {
        return tcpKeepCount;
    }

    public void setTcpKeepCount(int tcpKeepCount) {
        if (tcpKeepCount > 0) {
            this.tcpKeepCount = tcpKeepCount;
        } else {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepCount", tcpKeepCount, this.tcpKeepCount));
        }
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
                ", NIOFrontRW=" + NIOFrontRW +
                ", NIOBackendRW=" + NIOBackendRW +
                ", frontWorker=" + frontWorker +
                ", managerFrontWorker=" + managerFrontWorker +
                ", backendWorker=" + backendWorker +
                ", complexQueryWorker=" + complexQueryWorker +
                ", writeToBackendWorker=" + writeToBackendWorker +
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
                ", closeTimeout=" + closeTimeout +
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
                ", maxResultSet=" + maxResultSet +
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
                ", slowQueueOverflowPolicy=" + slowQueueOverflowPolicy +
                ", enableAlert=" + enableAlert +
                ", maxCharsPerColumn=" + maxCharsPerColumn +
                ", maxRowSizeToFile=" + maxRowSizeToFile +
                ", enableBatchLoadData=" + enableBatchLoadData +
                ", xaRetryCount=" + xaRetryCount +
                ", enableFlowControl=" + enableFlowControl +
                ", flowControlHighLevel=" + flowControlHighLevel +
                ", flowControlLowLevel=" + flowControlLowLevel +
                ", useOuterHa=" + useOuterHa +
                ", fakeMySQLVersion=" + fakeMySQLVersion +
                ", traceEndPoint=" + traceEndPoint +
                ", traceSamplerType=" + traceSamplerType +
                ", traceSamplerParam=" + traceSamplerParam +
                ", maxHeapTableSize=" + maxHeapTableSize +
                ", heapTableBufferChunkSize=" + heapTableBufferChunkSize +
                ", enableGeneralLog=" + enableGeneralLog +
                ", generalLogFile=" + generalLogFile +
                ", generalLogFileSize=" + generalLogFileSize +
                ", generalLogQueueSize=" + generalLogQueueSize +
                ", enableStatistic=" + enableStatistic +
                ", enableStatisticAnalysis=" + enableStatisticAnalysis +
                ", associateTablesByEntryByUserTableSize=" + associateTablesByEntryByUserTableSize +
                ", frontendByBackendByEntryByUserTableSize=" + frontendByBackendByEntryByUserTableSize +
                ", tableByUserByEntryTableSize=" + tableByUserByEntryTableSize +
                ", statisticQueueSize=" + statisticQueueSize +
                ", inSubQueryTransformToJoin=" + inSubQueryTransformToJoin +
                ", joinStrategyType=" + joinStrategyType +
                ", closeHeartBeatRecord=" + closeHeartBeatRecord +
                ", serverCertificateKeyStoreUrl=" + serverCertificateKeyStoreUrl +
                ", serverCertificateKeyStorePwd=" + serverCertificateKeyStorePwd +
                ", trustCertificateKeyStoreUrl=" + trustCertificateKeyStoreUrl +
                ", trustCertificateKeyStorePwd=" + trustCertificateKeyStorePwd +
                ", gmsslBothPfx=" + gmsslBothPfx +
                ", gmsslBothPfxPwd=" + gmsslBothPfxPwd +
                ", gmsslRcaPem=" + gmsslRcaPem +
                ", gmsslOcaPem=" + gmsslOcaPem +
                ", supportSSL=" + supportSSL +
                ", enableRoutePenetration=" + enableRoutePenetration +
                ", routePenetrationRules='" + routePenetrationRules + '\'' +
                ", enableSessionActiveRatioStat=" + enableSessionActiveRatioStat +
                ", enableConnectionAssociateThread=" + enableConnectionAssociateThread +
                ", district='" + district +
                ", dataCenter='" + dataCenter +
                ", groupConcatMaxLen='" + groupConcatMaxLen +
                ", releaseTimeout=" + releaseTimeout +
                ", enableAsyncRelease=" + enableAsyncRelease +
                ", xaIdCheckPeriod=" + xaIdCheckPeriod +
                ", enableBufferMonitor=" + enableMemoryBufferMonitor +
                ", enableBufferMonitorRecordPool=" + enableMemoryBufferMonitorRecordPool +
                ", enableSqlDumpLog=" + enableSqlDumpLog +
                ", sqlDumpLogBasePath='" + sqlDumpLogBasePath + '\'' +
                ", sqlDumpLogFileName='" + sqlDumpLogFileName + '\'' +
                ", sqlDumpLogCompressFilePattern='" + sqlDumpLogCompressFilePattern + '\'' +
                ", sqlDumpLogCompressFilePath='" + sqlDumpLogCompressFilePath + '\'' +
                ", sqlDumpLogOnStartupRotate=" + sqlDumpLogOnStartupRotate +
                ", sqlDumpLogSizeBasedRotate='" + sqlDumpLogSizeBasedRotate + '\'' +
                ", sqlDumpLogTimeBasedRotate=" + sqlDumpLogTimeBasedRotate +
                ", sqlDumpLogDeleteFileAge='" + sqlDumpLogDeleteFileAge + '\'' +
                ", queryForUpdateMaxRowsSize=" + queryForUpdateMaxRowsSize +
                ", tcpKeepIdle=" + tcpKeepIdle +
                ", tcpKeepInterval=" + tcpKeepInterval +
                ", tcpKeepCount=" + tcpKeepCount +
                ", heartbeatSqlExecuteTimeout=" + heartbeatSqlExecuteTimeout +
                "]";
    }

    public boolean isCloseHeartBeatRecord() {
        return closeHeartBeatRecord;
    }

    @SuppressWarnings("unused")
    public void setCloseHeartBeatRecord(boolean closeHeartBeatRecord) {
        this.closeHeartBeatRecord = closeHeartBeatRecord;
    }

    private void checkChineseProperty(String val, String name) {
        if (StringUtil.isBlank(val)) {
            problemReporter.warn("Property [ " + name + " ] " + val + " in bootstrap.cnf is illegal, Property [ " + name + " ]  not be null or empty");
            return;
        }
        int length = 11;
        if (val.length() > length) {
            problemReporter.warn("Property [ " + name + " ] " + val + " in bootstrap.cnf is illegal,the value contains a maximum of " + length + " characters");
        }

        String chinese = val.replaceAll(DBConverter.PATTERN_DB.toString(), "");
        if (Strings.isNullOrEmpty(chinese)) {
            return;
        }

        if (!StringUtil.isChinese(chinese)) {
            problemReporter.warn("Property [ " + name + " ] " + val + " in bootstrap.cnf is illegal,the " + Charset.defaultCharset().name() + " encoding is recommended, Property [ " + name + " ]  show be use  u4E00-u9FA5a-zA-Z_0-9\\-\\.");
        }
    }
}
