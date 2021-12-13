/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * show Sysconfig param detail info
 *
 * @author rainbow
 */
public final class ShowSysParam {
    private ShowSysParam() {
    }

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PARAM_NAME", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PARAM_VALUE", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("PARAM_DESCR", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.getPacketId();


        List<ParamInfo> paramValues = new ArrayList<>();
        paramValues.add(new ParamInfo("clusterEnable", ClusterConfig.getInstance().isClusterEnable() + "", "Whether enable the cluster mode"));
        if (ClusterConfig.getInstance().isClusterEnable()) {
            paramValues.add(new ParamInfo("clusterMode", ClusterConfig.getInstance().getClusterMode(), "clusterMode for dble cluster center"));
            paramValues.add(new ParamInfo("clusterIP", ClusterConfig.getInstance().getClusterIP(), "dble cluster center address, If clusterMode is zk, it is a full address with port"));
            paramValues.add(new ParamInfo("clusterPort", ClusterConfig.getInstance().getClusterPort() == 0 ? "" : ClusterConfig.getInstance().getClusterPort() + "", "dble cluster center address's port"));
            paramValues.add(new ParamInfo("rootPath", ClusterConfig.getInstance().getRootPath(), "dble cluster center's root path, the default value is /dble"));
            paramValues.add(new ParamInfo("clusterId", ClusterConfig.getInstance().getClusterId(), "dble cluster Id"));
            paramValues.add(new ParamInfo("needHaSync", ClusterConfig.getInstance().isNeedSyncHa() + "", "Whether dble use cluster ha, The default value is false"));
        }
        paramValues.add(new ParamInfo("showBinlogStatusTimeout", ClusterConfig.getInstance().getShowBinlogStatusTimeout() + "ms", "The time out from show @@binlog.status.The default value is 60000ms"));
        paramValues.add(new ParamInfo("sequenceHandlerType", ClusterConfig.getInstance().getSequenceHandlerType() > 4 || ClusterConfig.getInstance().getSequenceHandlerType() < 1 ? "Incorrect Sequence Type" : SEQUENCES[ClusterConfig.getInstance().getSequenceHandlerType()], "Global Sequence Type. The default is Local TimeStamp(like Snowflake)"));
        paramValues.add(new ParamInfo("sequenceStartTime", ClusterConfig.getInstance().getSequenceStartTime(), "valid for sequenceHandlerType=2 or 3, default is 2010-11-04 09:42:54"));
        paramValues.add(new ParamInfo("sequenceInstanceByZk", ClusterConfig.getInstance().isSequenceInstanceByZk() + "", "valid for sequenceHandlerType=3 and clusterMode is zk, default true"));

        SystemConfig sysConfig = SystemConfig.getInstance();
        paramValues.add(new ParamInfo("serverId", sysConfig.getServerId() + "", "serverID of machine which install dble, the default value is the machine IP"));
        paramValues.add(new ParamInfo("instanceName", sysConfig.getInstanceName() + "", "instanceName used to create xa transaction and unique key for cluster"));
        paramValues.add(new ParamInfo("instanceId", sysConfig.getInstanceId() + "", "instanceId used to when sequenceHandlerType=2 or (sequenceHandlerType=3 and sequenceInstanceByZk)"));
        paramValues.add(new ParamInfo("useOuterHa", sysConfig.isUseOuterHa() + "", "Whether use outer ha component. The default value is true and it will always true when clusterEnable=true.If no component in fact, nothing will happen."));
        paramValues.add(new ParamInfo("fakeMySQLVersion", sysConfig.getFakeMySQLVersion(), "MySQL Version showed in Client"));
        paramValues.add(new ParamInfo("bindIp", sysConfig.getBindIp() + "", "The host where the server is running. The default is 0.0.0.0"));
        paramValues.add(new ParamInfo("serverPort", sysConfig.getServerPort() + "", "User connection port. The default number is 8066"));
        paramValues.add(new ParamInfo("managerPort", sysConfig.getManagerPort() + "", "Manager connection port. The default number is 9066"));
        paramValues.add(new ParamInfo("processors", sysConfig.getProcessors() + "", "The size of frontend NIOProcessor, the default value is the number of processors available to the Java virtual machine"));
        paramValues.add(new ParamInfo("backendProcessors", sysConfig.getBackendProcessors() + "", "The size of backend NIOProcessor, the default value is the number of processors available to the Java virtual machine"));
        paramValues.add(new ParamInfo("processorExecutor", sysConfig.getProcessorExecutor() + "", "The size of fixed thread pool named of frontend businessExecutor,the default value is the number of processors available to the Java virtual machine * 2"));
        paramValues.add(new ParamInfo("backendProcessorExecutor", sysConfig.getBackendProcessorExecutor() + "", "The size of fixed thread pool named of backend businessExecutor,the default value is the number of processors available to the Java virtual machine * 2"));
        paramValues.add(new ParamInfo("complexExecutor", sysConfig.getComplexExecutor() + "", "The size of fixed thread pool named of writeToBackendExecutor,the default is the number of processors available to the Java virtual machine * 2"));
        paramValues.add(new ParamInfo("writeToBackendExecutor", sysConfig.getWriteToBackendExecutor() + "", "The executor for complex query.The default value is min(8, default value of processorExecutor)"));
        paramValues.add(new ParamInfo("serverBacklog", sysConfig.getServerBacklog() + "", "The NIO/AIO reactor backlog,the max of create connection request at one time.The default value is 2048"));
        paramValues.add(new ParamInfo("maxCon", sysConfig.getMaxCon() + "", "The number of max connections the server allowed "));
        paramValues.add(new ParamInfo("useCompression", sysConfig.getUseCompression() + "", "Whether the Compression is enable,The default number is 0 "));
        paramValues.add(new ParamInfo("usingAIO", sysConfig.getUsingAIO() + "", "Whether the AIO is enable, The default number is 0(use NIO instead)"));
        paramValues.add(new ParamInfo("useThreadUsageStat", sysConfig.getUseThreadUsageStat() + "", "Whether the thread usage statistics function is enabled.The default value is 0"));
        paramValues.add(new ParamInfo("usePerformanceMode", sysConfig.getUsePerformanceMode() + "", "Whether use the performance mode is enabled.The default value is 0"));
        paramValues.add(new ParamInfo("useCostTimeStat", sysConfig.getUseCostTimeStat() + "", "Whether the cost time of query can be track by Btrace.The default value is 0"));
        paramValues.add(new ParamInfo("maxCostStatSize", sysConfig.getMaxCostStatSize() + "", "The max cost total percentage.The default value is 100"));
        paramValues.add(new ParamInfo("costSamplePercent", sysConfig.getCostSamplePercent() + "", "The percentage of cost sample.The default value is 1"));
        paramValues.add(new ParamInfo("charset", sysConfig.getCharset() + "", "The initially charset of connection. The default is utf8mb4"));
        paramValues.add(new ParamInfo("maxPacketSize", sysConfig.getMaxPacketSize() + "", "The maximum size of one packet. The default is 4MB or (the Minimum value of all dbInstances - " + KeyVariables.MARGIN_PACKET_SIZE + ")."));
        paramValues.add(new ParamInfo("txIsolation", sysConfig.getTxIsolation() > 4 || sysConfig.getTxIsolation() < 1 ? "Incorrect isolation" : ISOLATION_LEVELS[sysConfig.getTxIsolation()], "The initially isolation level of the front end connection. The default is REPEATABLE_READ"));
        paramValues.add(new ParamInfo("autocommit", sysConfig.getAutocommit() + "", "The initially autocommit value.The default value is 1"));
        paramValues.add(new ParamInfo("idleTimeout", sysConfig.getIdleTimeout() + " ms", "The max allowed idle time of front connection. The connection will be closed if it is timed out after last read/write/heartbeat..The default value is 10min"));
        paramValues.add(new ParamInfo("checkTableConsistency", sysConfig.getCheckTableConsistency() + "", "Whether the consistency tableStructure check is enabled.The default value is 0"));
        paramValues.add(new ParamInfo("checkTableConsistencyPeriod", sysConfig.getCheckTableConsistencyPeriod() + "ms", "The period of consistency tableStructure check .The default value is 30*60*1000"));
        paramValues.add(new ParamInfo("processorCheckPeriod", sysConfig.getProcessorCheckPeriod() / 1000 + " Seconds", "The period between the jobs for cleaning the closed or overtime connections. The default is 1 second"));
        paramValues.add(new ParamInfo("sqlExecuteTimeout", sysConfig.getSqlExecuteTimeout() + " Seconds", "The max query executing time.If time out,the connection will be closed. The default is 300 seconds"));
        paramValues.add(new ParamInfo("recordTxn", sysConfig.getRecordTxn() + "", "Whether the transaction be recorded as a file,The default value is 0"));
        paramValues.add(new ParamInfo("transactionLogBaseDir", sysConfig.getTransactionLogBaseDir(), "The directory of the transaction record file,The default value is ./txlogs/"));
        paramValues.add(new ParamInfo("transactionLogBaseName", sysConfig.getTransactionLogBaseName(), "The name of the transaction record file.The default value is server-tx"));
        paramValues.add(new ParamInfo("transactionRotateSize", sysConfig.getTransactionRotateSize() + "M", "The max size of the transaction record file.The default value is 16M"));
        paramValues.add(new ParamInfo("xaRecoveryLogBaseDir", sysConfig.getXaRecoveryLogBaseDir(), "The directory of the xa transaction record file,The default value is ./xalogs/"));
        paramValues.add(new ParamInfo("xaRecoveryLogBaseName", sysConfig.getXaRecoveryLogBaseName(), "The name of the xa transaction record file.The default value is tmlog"));
        paramValues.add(new ParamInfo("xaSessionCheckPeriod", sysConfig.getXaSessionCheckPeriod() + "ms", "The xa transaction status check period.The default value is 1000ms"));
        paramValues.add(new ParamInfo("xaLogCleanPeriod", sysConfig.getXaLogCleanPeriod() + "ms", "The xa log clear period.The default value is 1000ms"));
        paramValues.add(new ParamInfo("xaRetryCount", sysConfig.getXaRetryCount() + "", "Indicates the number of background retries if the xa failed to commit/rollback.The default value is 0, retry infinitely"));
        paramValues.add(new ParamInfo("useJoinStrategy", sysConfig.isUseJoinStrategy() + "", "Whether nest loop join is enabled.The default value is false"));
        paramValues.add(new ParamInfo("nestLoopConnSize", sysConfig.getNestLoopConnSize() + "", "The nest loop temporary tables block number.The default value is 4"));
        paramValues.add(new ParamInfo("nestLoopRowsSize", sysConfig.getNestLoopRowsSize() + "", "The nest loop temporary tables rows for every block.The default value is 2000"));
        paramValues.add(new ParamInfo("otherMemSize", sysConfig.getOtherMemSize() + "M", "The additional size of memory can be used in a complex query.The default size is 4M"));
        paramValues.add(new ParamInfo("orderMemSize", sysConfig.getOrderMemSize() + "M", "The additional size of memory can be used in a complex query order.The default size is 4M"));
        paramValues.add(new ParamInfo("joinMemSize", sysConfig.getJoinMemSize() + "M", "The additional size of memory can be used in a complex query join.The default size is 4M"));
        paramValues.add(new ParamInfo("bufferPoolChunkSize", sysConfig.getBufferPoolChunkSize() + "B", "The chunk size of memory bufferPool. The min direct memory used for allocating"));
        paramValues.add(new ParamInfo("bufferPoolPageSize", sysConfig.getBufferPoolPageSize() + "B", "The page size of memory bufferPool. The max direct memory used for allocating"));
        paramValues.add(new ParamInfo("bufferPoolPageNumber", sysConfig.getBufferPoolPageNumber() + "", "The page number of memory bufferPool. The All bufferPool size is PageNumber * PageSize"));
        paramValues.add(new ParamInfo("mappedFileSize", sysConfig.getMappedFileSize() + "", "The Memory linked file size,when complex query resultSet is too large the Memory will be turned to file temporary"));
        paramValues.add(new ParamInfo("useSqlStat", sysConfig.getUseSqlStat() + "", "Whether the SQL statistics function is enable or not.The default value is 1"));
        paramValues.add(new ParamInfo("sqlRecordCount", sysConfig.getSqlRecordCount() + "", "The slow SQL statistics limit,if the slow SQL record is large than the size,the record will be clear.The default value is 10"));
        paramValues.add(new ParamInfo("maxResultSet", sysConfig.getMaxResultSet() + "B", "The large resultSet SQL standard.The default value is 512*1024B"));
        paramValues.add(new ParamInfo("bufferUsagePercent", sysConfig.getBufferUsagePercent() + "%", "Large result set cleanup trigger percentage.The default value is 80"));
        paramValues.add(new ParamInfo("clearBigSQLResultSetMapMs", sysConfig.getClearBigSQLResultSetMapMs() + "ms", "The period for clear the large resultSet SQL statistics.The default value is 6000000ms"));
        paramValues.add(new ParamInfo("frontSocketSoRcvbuf", sysConfig.getFrontSocketSoRcvbuf() + "B", "The buffer size of frontend receive socket.The default value is 1024*1024"));
        paramValues.add(new ParamInfo("frontSocketSoSndbuf", sysConfig.getFrontSocketSoSndbuf() + "B", "The buffer size of frontend send socket.The default value is 1024*1024*4"));
        paramValues.add(new ParamInfo("frontSocketNoDelay", sysConfig.getFrontSocketNoDelay() + "", "The frontend nagle is disabled.The default value is 1"));
        paramValues.add(new ParamInfo("backSocketSoRcvbuf", sysConfig.getBackSocketSoRcvbuf() + "B", "The buffer size of backend receive socket.The default value is 1024*1024*4"));
        paramValues.add(new ParamInfo("backSocketSoSndbuf", sysConfig.getBackSocketSoSndbuf() + "B", "The buffer size of backend send socket.The default value is 1024*1024"));
        paramValues.add(new ParamInfo("backSocketNoDelay", sysConfig.getBackSocketNoDelay() + "", "The backend nagle is disabled.The default value is 1"));
        paramValues.add(new ParamInfo("viewPersistenceConfBaseDir", sysConfig.getViewPersistenceConfBaseDir(), "The directory of the view record file,The default value is ./viewConf/"));
        paramValues.add(new ParamInfo("viewPersistenceConfBaseName", sysConfig.getViewPersistenceConfBaseName(), "The name of the view record file.The default value is viewJson"));
        paramValues.add(new ParamInfo("joinQueueSize", sysConfig.getJoinQueueSize() + "", "Size of join queue,Avoid using too much memory"));
        paramValues.add(new ParamInfo("mergeQueueSize", sysConfig.getMergeQueueSize() + "", "Size of merge queue,Avoid using too much memory"));
        paramValues.add(new ParamInfo("orderByQueueSize", sysConfig.getOrderByQueueSize() + "", "Size of order by queue,Avoid using too much memory"));
        paramValues.add(new ParamInfo("enableSlowLog", sysConfig.getEnableSlowLog() + "", "Enable Slow Query Log"));
        paramValues.add(new ParamInfo("slowLogBaseDir", sysConfig.getSlowLogBaseDir() + "", "The directory of slow query log,The default value is ./slowlogs/"));
        paramValues.add(new ParamInfo("slowLogBaseName", sysConfig.getSlowLogBaseName() + "", "The name of the slow query log.The default value is slow-query"));
        paramValues.add(new ParamInfo("flushSlowLogPeriod", sysConfig.getFlushSlowLogPeriod() + "s", "The period for flushing log to disk, the default is 1 second"));
        paramValues.add(new ParamInfo("flushSlowLogSize", sysConfig.getFlushSlowLogSize() + "", "The max size for flushing log to disk, the default is 1000 "));
        paramValues.add(new ParamInfo("sqlSlowTime", sysConfig.getSqlSlowTime() + "ms", "The threshold of Slow Query, the default is 100ms"));
        paramValues.add(new ParamInfo("maxCharsPerColumn", sysConfig.getMaxCharsPerColumn() + "", "The maximum number of characters allowed for per column when load data.The default value is 65535"));
        paramValues.add(new ParamInfo("maxRowSizeToFile", sysConfig.getMaxRowSizeToFile() + "", "The maximum row size,if over this value,row data will be saved to file when load data.The default value is 10000"));
        paramValues.add(new ParamInfo("enableFlowControl", sysConfig.isEnableFlowControl() + "", "Whether use flow control feature"));
        paramValues.add(new ParamInfo("flowControlStartThreshold", sysConfig.getFlowControlStartThreshold() + "", "The start threshold of write queue to start the flow control"));
        paramValues.add(new ParamInfo("flowControlStopThreshold", sysConfig.getFlowControlStopThreshold() + "", "The recover threshold of write queue to stop the flow control"));


        for (ParamInfo info : paramValues) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(info.paramName, c.getCharset().getResults()));
            row.add(StringUtil.encode(info.paramValue, c.getCharset().getResults()));
            row.add(StringUtil.encode(info.paramDesc, c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }


    private static final String[] ISOLATION_LEVELS = {"", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"};
    private static final String[] SEQUENCES = {"", "Offset-Step stored in MySQL", "Local TimeStamp(like Snowflake)", "ZooKeeper/Local TimeStamp(like Snowflake)", "Offset-Step stored in ZooKeeper"};

    private static class ParamInfo {
        private String paramName;
        private String paramValue;
        private String paramDesc;

        ParamInfo(String paramName, String paramValue, String paramDesc) {
            this.paramName = paramName;
            this.paramValue = paramValue;
            this.paramDesc = paramDesc;
        }
    }
}
