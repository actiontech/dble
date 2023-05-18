/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;


import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.xa.XaCheckHandler;
import com.actiontech.dble.buffer.MemoryBufferMonitor;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.ParamInfo;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.ssl.GMSslWrapper;
import com.actiontech.dble.net.ssl.OpenSSLWrapper;
import com.actiontech.dble.net.ssl.SSLWrapperRegistry;
import com.actiontech.dble.server.status.GeneralLog;
import com.actiontech.dble.server.status.LoadDataBatch;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.status.SqlDumpLog;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.stat.FrontActiveRatioStat;

import java.util.ArrayList;
import java.util.List;

import static com.actiontech.dble.cluster.ClusterController.CONFIG_MODE_ZK;

public final class SystemParams {
    private static final SystemParams INSTANCE = new SystemParams();

    public static SystemParams getInstance() {
        return INSTANCE;
    }

    private List<ParamInfo> readOnlyParams = new ArrayList<>();


    public List<ParamInfo> getReadOnlyParams() {
        return readOnlyParams;
    }

    private SystemParams() {

        readOnlyParams.add(new ParamInfo("clusterEnable", ClusterConfig.getInstance().isClusterEnable() + "", "Whether enable the cluster mode"));
        if (ClusterConfig.getInstance().isClusterEnable()) {
            readOnlyParams.add(new ParamInfo("clusterMode", ClusterConfig.getInstance().getClusterMode(), "ClusterMode for dble cluster center"));
            readOnlyParams.add(new ParamInfo("clusterIP", ClusterConfig.getInstance().getClusterIP(), "Dble cluster center address, If clusterMode is zk, it is a full address with port"));
            if (!ClusterConfig.getInstance().getClusterMode().equalsIgnoreCase(CONFIG_MODE_ZK)) {
                readOnlyParams.add(new ParamInfo("clusterPort", ClusterConfig.getInstance().getClusterPort() == 0 ? "" : ClusterConfig.getInstance().getClusterPort() + "", "Dble cluster center address's port"));
                readOnlyParams.add(new ParamInfo("grpcTimeout", ClusterConfig.getInstance().getGrpcTimeout() + "", "Under non-zk cluster configuration, the timeout period for calling the interface"));
            }
            readOnlyParams.add(new ParamInfo("rootPath", ClusterConfig.getInstance().getRootPath(), "Dble cluster center's root path, the default value is /dble"));
            readOnlyParams.add(new ParamInfo("clusterId", ClusterConfig.getInstance().getClusterId(), "Dble cluster Id"));
            readOnlyParams.add(new ParamInfo("needHaSync", ClusterConfig.getInstance().isNeedSyncHa() + "", "Whether dble use cluster ha, The default value is false"));
        }
        readOnlyParams.add(new ParamInfo("showBinlogStatusTimeout", ClusterConfig.getInstance().getShowBinlogStatusTimeout() + "ms", "The time out from show @@binlog.status.The default value is 60000ms"));
        String[] sequences = {"", "Offset-Step stored in MySQL", "Local TimeStamp(like Snowflake)", "ZooKeeper/Local TimeStamp(like Snowflake)", "Offset-Step stored in ZooKeeper"};
        readOnlyParams.add(new ParamInfo("sequenceHandlerType", ClusterConfig.getInstance().getSequenceHandlerType() > 4 || ClusterConfig.getInstance().getSequenceHandlerType() < 1 ? "Incorrect Sequence Type" : sequences[ClusterConfig.getInstance().getSequenceHandlerType()], "Global Sequence Type. The default is Local TimeStamp(like Snowflake)"));
        readOnlyParams.add(new ParamInfo("sequenceStartTime", ClusterConfig.getInstance().getSequenceStartTime(), "Valid for sequenceHandlerType=2 or 3, default is 2010-11-04 09:42:54"));
        readOnlyParams.add(new ParamInfo("sequenceInstanceByZk", ClusterConfig.getInstance().isSequenceInstanceByZk() + "", "Valid for sequenceHandlerType=3 and clusterMode is zk, default true"));

        SystemConfig sysConfig = SystemConfig.getInstance();
        readOnlyParams.add(new ParamInfo("serverId", sysConfig.getServerId() + "", "ServerID of machine which install dble, the default value is the machine IP"));
        readOnlyParams.add(new ParamInfo("instanceName", sysConfig.getInstanceName() + "", "InstanceName used to create xa transaction and unique key for cluster"));
        readOnlyParams.add(new ParamInfo("instanceId", sysConfig.getInstanceId() + "", "InstanceId used to when sequenceHandlerType=2 or (sequenceHandlerType=3 and sequenceInstanceByZk)"));
        readOnlyParams.add(new ParamInfo("useOuterHa", sysConfig.isUseOuterHa() + "", "Whether use outer ha component. The default value is true and it will always true when clusterEnable=true.If no component in fact, nothing will happen."));
        readOnlyParams.add(new ParamInfo("fakeMySQLVersion", sysConfig.getFakeMySQLVersion(), "MySQL Version showed in Client"));
        readOnlyParams.add(new ParamInfo("bindIp", sysConfig.getBindIp() + "", "The host where the server is running. The default is 0.0.0.0"));
        readOnlyParams.add(new ParamInfo("serverPort", sysConfig.getServerPort() + "", "User connection port. The default number is 8066"));
        readOnlyParams.add(new ParamInfo("managerPort", sysConfig.getManagerPort() + "", "Manager connection port. The default number is 9066"));
        readOnlyParams.add(new ParamInfo("NIOFrontRW", sysConfig.getNIOFrontRW() + "", "The size of frontend NIOProcessor, the default value is the number of processors available to the Java virtual machine"));
        readOnlyParams.add(new ParamInfo("NIOBackendRW", sysConfig.getNIOBackendRW() + "", "The size of backend NIOProcessor, the default value is the number of processors available to the Java virtual machine"));
        readOnlyParams.add(new ParamInfo("frontWorker", sysConfig.getFrontWorker() + "", "The size of fixed thread pool named of frontWorker, the default value is the number of processors available to the Java virtual machine * 2"));
        readOnlyParams.add(new ParamInfo("backendWorker", sysConfig.getBackendWorker() + "", "The size of fixed thread pool named of backendWorker,the default value is the number of processors available to the Java virtual machine * 2"));
        readOnlyParams.add(new ParamInfo("complexQueryWorker", sysConfig.getComplexQueryWorker() + "", "The size of fixed thread pool named of complexQueryWorker,the default is the number of processors available to the Java virtual machine * 2"));
        readOnlyParams.add(new ParamInfo("writeToBackendWorker", sysConfig.getWriteToBackendWorker() + "", "The executor for writeToBackendWorker.The default value is min(8, default value of NIOFrontRW)"));
        readOnlyParams.add(new ParamInfo("serverBacklog", sysConfig.getServerBacklog() + "", "The NIO/AIO reactor backlog,the max of create connection request at one time. The default value is 2048"));
        readOnlyParams.add(new ParamInfo("maxCon", sysConfig.getMaxCon() + "", "The number of max connections the server allowed, 0 means number of max connections attributes is not limited"));
        readOnlyParams.add(new ParamInfo("useCompression", sysConfig.getUseCompression() + "", "Whether the Compression is enable, the default number is 0"));
        readOnlyParams.add(new ParamInfo("usingAIO", sysConfig.getUsingAIO() + "", "Whether the AIO is enable, the default number is 0(use NIO instead)"));
        readOnlyParams.add(new ParamInfo("useThreadUsageStat", sysConfig.getUseThreadUsageStat() + "", "Whether the thread usage statistics function is enabled. The default value is 0"));
        readOnlyParams.add(new ParamInfo("usePerformanceMode", sysConfig.getUsePerformanceMode() + "", "Whether use the performance mode is enabled. The default value is 0"));
        readOnlyParams.add(new ParamInfo("useCostTimeStat", sysConfig.getUseCostTimeStat() + "", "Whether the cost time of query can be track by Btrace. The default value is 0"));
        readOnlyParams.add(new ParamInfo("maxCostStatSize", sysConfig.getMaxCostStatSize() + "", "The max cost total percentage. The default value is 100"));
        readOnlyParams.add(new ParamInfo("costSamplePercent", sysConfig.getCostSamplePercent() + "%", "The percentage of cost sample. The default value is 1%"));
        readOnlyParams.add(new ParamInfo("charset", sysConfig.getCharset() + "", "The initially charset of connection. The default is utf8mb4"));
        readOnlyParams.add(new ParamInfo("maxPacketSize", sysConfig.getMaxPacketSize() + "B", "The maximum size of one packet. The default is 4MB or (the Minimum value of all dbInstances - " + KeyVariables.MARGIN_PACKET_SIZE + ")."));
        String[] isolationLevels = {"", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"};
        readOnlyParams.add(new ParamInfo("txIsolation", sysConfig.getTxIsolation() > 4 || sysConfig.getTxIsolation() < 1 ? "Incorrect isolation" : isolationLevels[sysConfig.getTxIsolation()], "The initially isolation level of the front end connection. The default is REPEATABLE_READ"));
        readOnlyParams.add(new ParamInfo("autocommit", sysConfig.getAutocommit() + "", "The initially autocommit value.The default value is 1"));
        readOnlyParams.add(new ParamInfo("idleTimeout", sysConfig.getIdleTimeout() + "ms", "The max allowed idle time of front connection. The connection will be closed if it is timed out after last read/write/heartbeat. The default value is 10min"));
        readOnlyParams.add(new ParamInfo("checkTableConsistency", sysConfig.getCheckTableConsistency() + "", "Whether the consistency tableStructure check is enabled. The default value is 0"));
        readOnlyParams.add(new ParamInfo("checkTableConsistencyPeriod", sysConfig.getCheckTableConsistencyPeriod() + "ms", "The period of consistency tableStructure check. The default value is 1800000ms(means 30minutes=30*60*1000)"));
        readOnlyParams.add(new ParamInfo("processorCheckPeriod", sysConfig.getProcessorCheckPeriod() + "ms", "The period between the jobs for cleaning the closed or overtime connections. The default is 1000ms"));
        readOnlyParams.add(new ParamInfo("sqlExecuteTimeout", sysConfig.getSqlExecuteTimeout() + "s", "The max query executing time.If time out,the connection will be closed. The default is 300 seconds"));
        readOnlyParams.add(new ParamInfo("recordTxn", sysConfig.getRecordTxn() + "", "Whether the transaction be recorded as a file, the default value is 0"));
        readOnlyParams.add(new ParamInfo("transactionLogBaseDir", sysConfig.getTransactionLogBaseDir(), "The directory of the transaction record file, the default value is ./txlogs/"));
        readOnlyParams.add(new ParamInfo("transactionLogBaseName", sysConfig.getTransactionLogBaseName(), "The name of the transaction record file. The default value is server-tx"));
        readOnlyParams.add(new ParamInfo("transactionRotateSize", sysConfig.getTransactionRotateSize() + "M", "The max size of the transaction record file. The default value is 16M"));
        readOnlyParams.add(new ParamInfo("xaRecoveryLogBaseDir", sysConfig.getXaRecoveryLogBaseDir(), "The directory of the xa transaction record file, the default value is ./xalogs/"));
        readOnlyParams.add(new ParamInfo("xaRecoveryLogBaseName", sysConfig.getXaRecoveryLogBaseName(), "The name of the xa transaction record file. The default value is xalog"));
        readOnlyParams.add(new ParamInfo("xaSessionCheckPeriod", sysConfig.getXaSessionCheckPeriod() + "ms", "The xa transaction status check period. The default value is 1000ms"));
        readOnlyParams.add(new ParamInfo("xaLogCleanPeriod", sysConfig.getXaLogCleanPeriod() + "ms", "The xa log clear period. The default value is 1000ms"));
        readOnlyParams.add(new ParamInfo("xaRetryCount", sysConfig.getXaRetryCount() + "", "Indicates the number of background retries if the xa failed to commit/rollback. The default value is 0, retry infinitely"));
        readOnlyParams.add(new ParamInfo("useJoinStrategy", sysConfig.isUseJoinStrategy() + "", "Whether nest loop join is enabled. The default value is false"));
        readOnlyParams.add(new ParamInfo("nestLoopConnSize", sysConfig.getNestLoopConnSize() + "", "The nest loop temporary tables block number. The default value is 4"));
        readOnlyParams.add(new ParamInfo("nestLoopRowsSize", sysConfig.getNestLoopRowsSize() + "", "The nest loop temporary tables rows for every block. The default value is 2000"));
        readOnlyParams.add(new ParamInfo("otherMemSize", sysConfig.getOtherMemSize() + "M", "The additional size of memory can be used in a complex query. The default size is 4M"));
        readOnlyParams.add(new ParamInfo("orderMemSize", sysConfig.getOrderMemSize() + "M", "The additional size of memory can be used in a complex query order. The default size is 4M"));
        readOnlyParams.add(new ParamInfo("joinMemSize", sysConfig.getJoinMemSize() + "M", "The additional size of memory can be used in a complex query join. The default size is 4M"));
        readOnlyParams.add(new ParamInfo("bufferPoolChunkSize", sysConfig.getBufferPoolChunkSize() + "B", "The chunk size of memory bufferPool. The min direct memory used for allocating"));
        readOnlyParams.add(new ParamInfo("bufferPoolPageSize", sysConfig.getBufferPoolPageSize() + "B", "The page size of memory bufferPool. The max direct memory used for allocating"));
        readOnlyParams.add(new ParamInfo("bufferPoolPageNumber", sysConfig.getBufferPoolPageNumber() + "", "The page number of memory bufferPool. The All bufferPool size is PageNumber * PageSize"));
        readOnlyParams.add(new ParamInfo("mappedFileSize", sysConfig.getMappedFileSize() + "B", "The Memory linked file size,when complex query resultSet is too large the Memory will be turned to file temporary"));
        readOnlyParams.add(new ParamInfo("maxResultSet", sysConfig.getMaxResultSet() + "B", "The large resultSet SQL standard. The default value is 524288B(means 512*1024)"));
        readOnlyParams.add(new ParamInfo("frontSocketSoRcvbuf", sysConfig.getFrontSocketSoRcvbuf() + "B", "The buffer size of frontend receive socket. The default value is 1048576B(means 1024*1024)"));
        readOnlyParams.add(new ParamInfo("frontSocketSoSndbuf", sysConfig.getFrontSocketSoSndbuf() + "B", "The buffer size of frontend send socket. The default value is 4194304B(means 1024*1024*4)"));
        readOnlyParams.add(new ParamInfo("frontSocketNoDelay", sysConfig.getFrontSocketNoDelay() + "", "The frontend nagle is disabled. The default value is 1"));
        readOnlyParams.add(new ParamInfo("backSocketSoRcvbuf", sysConfig.getBackSocketSoRcvbuf() + "B", "The buffer size of backend receive socket. The default value is 4194304B(means 1024*1024*4)"));
        readOnlyParams.add(new ParamInfo("backSocketSoSndbuf", sysConfig.getBackSocketSoSndbuf() + "B", "The buffer size of backend send socket. The default value is 1048576B(means 1024*1024)"));
        readOnlyParams.add(new ParamInfo("backSocketNoDelay", sysConfig.getBackSocketNoDelay() + "", "The backend nagle is disabled. The default value is 1"));
        readOnlyParams.add(new ParamInfo("viewPersistenceConfBaseDir", sysConfig.getViewPersistenceConfBaseDir(), "The directory of the view record file, the default value is ./viewConf/"));
        readOnlyParams.add(new ParamInfo("viewPersistenceConfBaseName", sysConfig.getViewPersistenceConfBaseName(), "The name of the view record file. The default value is viewJson"));
        readOnlyParams.add(new ParamInfo("joinQueueSize", sysConfig.getJoinQueueSize() + "", "Size of join queue,Avoid using too much memory"));
        readOnlyParams.add(new ParamInfo("mergeQueueSize", sysConfig.getMergeQueueSize() + "", "Size of merge queue,Avoid using too much memory"));
        readOnlyParams.add(new ParamInfo("orderByQueueSize", sysConfig.getOrderByQueueSize() + "", "Size of order by queue, avoid using too much memory"));
        readOnlyParams.add(new ParamInfo("slowLogBaseDir", sysConfig.getSlowLogBaseDir() + "", "The directory of slow query log, the default value is ./slowlogs/"));
        readOnlyParams.add(new ParamInfo("slowLogBaseName", sysConfig.getSlowLogBaseName() + "", "The name of the slow query log. The default value is slow-query"));
        readOnlyParams.add(new ParamInfo("maxCharsPerColumn", sysConfig.getMaxCharsPerColumn() + "", "The maximum number of characters allowed for per column when load data. The default value is 65535"));
        readOnlyParams.add(new ParamInfo("enableTrace", TraceManager.isEnable() + "", "Whether the trace Jaeger is enabled"));
        readOnlyParams.add(new ParamInfo("traceEndPoint", sysConfig.getTraceEndPoint(), "The trace Jaeger server endPoint"));
        readOnlyParams.add(new ParamInfo("traceSamplerType", TraceManager.getSamplerType(), "The trace Jaeger sampler type. The default type is 'const'"));
        readOnlyParams.add(new ParamInfo("traceSamplerParam", TraceManager.getSamplerParam(), "The trace Jaeger sampler param"));
        readOnlyParams.add(new ParamInfo("generalLogFileSize", GeneralLog.getInstance().getGeneralLogFileSize() + "M", "The max size of the general log file. The default value is 16M"));
        readOnlyParams.add(new ParamInfo("generalLogQueueSize", GeneralLog.getInstance().getGeneralLogQueueSize() + "", "Sets the queue size for consuming general log, value must not be less than 1 and must be a power of 2, the default value is 4096"));
        readOnlyParams.add(new ParamInfo("enableCursor", Boolean.valueOf(sysConfig.isEnableCursor()).toString(), "Whether the server-side cursor  is enable or not. The default value is false"));
        readOnlyParams.add(new ParamInfo("maxHeapTableSize", sysConfig.getMaxHeapTableSize() + "B", "Used for temp table persistence of cursor, temp table which size larger than that will save to disk."));
        readOnlyParams.add(new ParamInfo("heapTableBufferChunkSize", sysConfig.getHeapTableBufferChunkSize() + "B", "Used for temp table persistence of cursor, setting for read-buffer size."));
        readOnlyParams.add(new ParamInfo("statisticQueueSize", StatisticManager.getInstance().getStatisticQueueSize() + "", "Sets the queue size for statistic, value must not be less than 1 and must be a power of 2,the default value is 4096"));
        readOnlyParams.add(new ParamInfo("inSubQueryTransformToJoin", sysConfig.isInSubQueryTransformToJoin() + "", "The inSubQuery is transformed into the join ,the default value is false"));
        readOnlyParams.add(new ParamInfo("rwStickyTime", sysConfig.getRwStickyTime() + "ms", "For rwSplitUser, Implement stickiness for read and write instances, the default value is 1000ms"));
        readOnlyParams.add(new ParamInfo("joinStrategyType", sysConfig.getJoinStrategyType() + "", "Nest loop strategy type. The default value is -1"));
        readOnlyParams.add(new ParamInfo("closeHeartBeatRecord", sysConfig.isCloseHeartBeatRecord() + "", "close heartbeat record. if closed, `show @@dbinstance.synstatus`,`show @@dbinstance.syndetail`,`show @@heartbeat.detail` will be empty and `show @@heartbeat`'s EXECUTE_TIME will be '-' .The default value is false"));
        readOnlyParams.add(new ParamInfo("enableRoutePenetration", sysConfig.isEnableRoutePenetration() + "", "Whether enable route penetration.The default value is 0"));
        readOnlyParams.add(new ParamInfo("routePenetrationRules", sysConfig.getRoutePenetrationRules() + "", "The config of route penetration.The default value is ''"));
        readOnlyParams.add(new ParamInfo("enableSessionActiveRatioStat", FrontActiveRatioStat.getInstance().isEnable() ? "1" : "0", "Whether frontend connection activity ratio statistics are enabled. The default value is 1."));
        readOnlyParams.add(new ParamInfo("enableConnectionAssociateThread", ConnectionAssociateThreadManager.getInstance().isEnable() ? "1" : "0", "Whether to open frontend connection and backend connection are associated with threads. The default value is 1."));
        readOnlyParams.add(new ParamInfo("isSupportSSL", SystemConfig.getInstance().isSupportSSL() + "", "isSupportSSL in configuration"));
        readOnlyParams.add(new ParamInfo("isSupportOpenSSL", (SSLWrapperRegistry.getInstance(OpenSSLWrapper.PROTOCOL) != null) + "", "Whether OpenSSL is actually supported"));
        readOnlyParams.add(new ParamInfo("serverCertificateKeyStoreUrl", SystemConfig.getInstance().getServerCertificateKeyStoreUrl() + "", "Service certificate required of OpenSSL"));
        readOnlyParams.add(new ParamInfo("trustCertificateKeyStoreUrl", SystemConfig.getInstance().getTrustCertificateKeyStoreUrl() + "", "Trust certificate required of OpenSSL"));
        readOnlyParams.add(new ParamInfo("isSupportGMSSL", (SSLWrapperRegistry.getInstance(GMSslWrapper.PROTOCOL) != null) + "", "Whether GMSSL is actually supported"));
        readOnlyParams.add(new ParamInfo("gmsslBothPfx", SystemConfig.getInstance().getGmsslBothPfx() + "", "National secret dual certificate/private key file in PFX format"));
        readOnlyParams.add(new ParamInfo("gmsslRcaPem", SystemConfig.getInstance().getGmsslRcaPem() + "", "Root certificate of GMSSL"));
        readOnlyParams.add(new ParamInfo("gmsslOcaPem", SystemConfig.getInstance().getGmsslOcaPem() + "", "Secondary certificate of GMSSL"));
        readOnlyParams.add(new ParamInfo("district", sysConfig.getDistrict() + "", "The location of the DBLE"));
        readOnlyParams.add(new ParamInfo("dataCenter", sysConfig.getDataCenter() + "", "The data center where the DBLE resides"));
        readOnlyParams.add(new ParamInfo("groupConcatMaxLen", sysConfig.getGroupConcatMaxLen() + "", "The maximum permitted result length in bytes for the GROUP_CONCAT() function. The default is 1024."));
        readOnlyParams.add(new ParamInfo("enableAsyncRelease", sysConfig.getEnableAsyncRelease() + "", "Whether enable async release . default value is 1(on)."));
        readOnlyParams.add(new ParamInfo("releaseTimeout", sysConfig.getReleaseTimeout() + "", "time wait for release ,unit is ms,  default value is 10ms"));

        readOnlyParams.add(new ParamInfo("sqlDumpLogBasePath", SqlDumpLog.getInstance().getSqlDumpLogBasePath() + "", "The base path of sqldump log, the default value is 'sqldump'"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogFileName", SqlDumpLog.getInstance().getSqlDumpLogFileName() + "", "The sqldump log file name, the default value is 'sqldump.log'"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogCompressFilePattern", SqlDumpLog.getInstance().getSqlDumpLogCompressFilePattern() + "", "The compression of sqldump log file, the default value is '${date:yyyy-MM}/sqldump-%d{MM-dd}-%i.log.gz'"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogOnStartupRotate", SqlDumpLog.getInstance().getSqlDumpLogOnStartupRotate() + "", "The onStartup of rotate policy, the default value is 1; -1 said not to participate in the strategy"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogSizeBasedRotate", SqlDumpLog.getInstance().getSqlDumpLogSizeBasedRotate() + "", "The sizeBased of rotate policy, the default value is '50 MB'; default unit is byte"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogTimeBasedRotate", SqlDumpLog.getInstance().getSqlDumpLogTimeBasedRotate() + "", "The timeBased of rotate policy, the default value is 1; -1 said not to participate in the strategy"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogDeleteFileAge", SqlDumpLog.getInstance().getSqlDumpLogDeleteFileAge() + "", "The expiration time deletion strategy, the default value is '90d'"));
        readOnlyParams.add(new ParamInfo("sqlDumpLogCompressFilePath", SqlDumpLog.getInstance().getSqlDumpLogCompressFilePath() + "", "The compression of sqldump log file path, the default value is '*/sqldump-*.log.gz'"));

        readOnlyParams.add(new ParamInfo("enableMemoryBufferMonitorRecordPool", sysConfig.getEnableMemoryBufferMonitorRecordPool() + "", "Whether record the connection pool memory if the memory buffer monitor is ON. the default value is 1(ON)."));
        readOnlyParams.add(new ParamInfo("queryForUpdateMaxRowsSize", sysConfig.getQueryForUpdateMaxRowsSize() + "", "The maximum number of rows in the select result set when update multi-table splitting is delivered. The default value is 20000"));

        readOnlyParams.add(new ParamInfo("tcpKeepIdle", sysConfig.getTcpKeepIdle() + "", "TCP probes a connection that has been idle for some amount of time,unit is s, default value is 30s"));
        readOnlyParams.add(new ParamInfo("tcpKeepInterval", sysConfig.getTcpKeepInterval() + "", "Keep-Alive retransmission interval time,unit is s,  default value is 10s"));
        readOnlyParams.add(new ParamInfo("tcpKeepCount", sysConfig.getTcpKeepCount() + "", "Keep-Alive retransmission maximum limit, default value is 3"));
    }

    public List<ParamInfo> getVolatileParams() {
        List<ParamInfo> params = new ArrayList<>();
        params.add(new ParamInfo("enableFlowControl", FlowController.isEnableFlowControl() + "", "Whether use flow control feature"));
        params.add(new ParamInfo("flowControlHighLevel", FlowController.getFlowHighLevel() + "", "The byte size of write queue to start the flow control"));
        params.add(new ParamInfo("flowControlLowLevel", FlowController.getFlowLowLevel() + "", "The byte size of write queue to stop the flow control"));
        params.add(new ParamInfo("enableSlowLog", SlowQueryLog.getInstance().isEnableSlowLog() ? "1" : "0", "Enable Slow Query Log"));
        params.add(new ParamInfo("sqlSlowTime", SlowQueryLog.getInstance().getSlowTime() + "ms", "The threshold of Slow Query, the default is 100ms"));
        params.add(new ParamInfo("flushSlowLogPeriod", SlowQueryLog.getInstance().getFlushPeriod() + "s", "The period for flushing log to disk, the default is 1 second"));
        params.add(new ParamInfo("flushSlowLogSize", SlowQueryLog.getInstance().getFlushSize() + "", "The max size for flushing log to disk, the default is 1000"));
        params.add(new ParamInfo("slowQueueOverflowPolicy", SlowQueryLog.getInstance().getQueueOverflowPolicy() + "", "Slow log queue overflow policy, the default is 2"));
        params.add(new ParamInfo("enableAlert", AlertUtil.isEnable() ? "1" : "0", "Enable or disable alert"));
        params.add(new ParamInfo("capClientFoundRows", CapClientFoundRows.getInstance().isEnableCapClientFoundRows() + "", "Whether to turn on EOF_Packet to return found rows, the default value is false"));
        params.add(new ParamInfo("maxRowSizeToFile", LoadDataBatch.getInstance().getSize() + "", "The maximum row size,if over this value,row data will be saved to file when load data.The default value is 100000"));
        params.add(new ParamInfo("enableBatchLoadData", LoadDataBatch.getInstance().isEnableBatchLoadData() ? "1" : "0", "Enable Batch Load Data. The default value is 0(false)"));
        params.add(new ParamInfo("enableGeneralLog", GeneralLog.getInstance().isEnableGeneralLog() ? "1" : "0", "Enable general log"));
        params.add(new ParamInfo("generalLogFile", GeneralLog.getInstance().getGeneralLogFile(), "The path of general log, the default value is ./general/general.log"));
        params.add(new ParamInfo("enableStatistic", StatisticManager.getInstance().isEnable() ? "1" : "0", "Enable statistic sql, the default is 0(false)"));
        params.add(new ParamInfo("enableStatisticAnalysis", StatisticManager.getInstance().isEnableAnalysis() ? "1" : "0", "Enable statistic analysis sql('show @@sql.sum.user/table' or 'show @@sql.condition'), the default is 0(false)"));
        params.add(new ParamInfo("associateTablesByEntryByUserTableSize", StatisticManager.getInstance().getAssociateTablesByEntryByUserTableSize() + "", "AssociateTablesByEntryByUser table size, the default is 1024"));
        params.add(new ParamInfo("frontendByBackendByEntryByUserTableSize", StatisticManager.getInstance().getFrontendByBackendByEntryByUserTableSize() + "", "FrontendByBackendByEntryByUser table size, the default is 1024"));
        params.add(new ParamInfo("tableByUserByEntryTableSize", StatisticManager.getInstance().getTableByUserByEntryTableSize() + "", "TableByUserByEntry table size, the default is 1024"));
        params.add(new ParamInfo("sqlLogTableSize", StatisticManager.getInstance().getSqlLogSize() + "", "SqlLog table size, the default is 1024"));
        params.add(new ParamInfo("samplingRate", StatisticManager.getInstance().getSamplingRate() + "", "Sampling rate, the default is 100, it is a percentage"));
        params.add(new ParamInfo("xaIdCheckPeriod", XaCheckHandler.getXaIdCheckPeriod() + "s", "The period for check xaId, the default is 300 second"));
        params.add(new ParamInfo("enableSqlDumpLog", SqlDumpLog.getInstance().getEnableSqlDumpLog() + "", "Whether enable sqlDumpLog, the default value is 0(off)"));

        params.add(new ParamInfo("enableMemoryBufferMonitor", MemoryBufferMonitor.getInstance().isEnable() ? "1" : "0", "Whether enable memory buffer monitor, enable this option will cost a lot of  resources. the default value is 0(off)"));

        return params;
    }
}
