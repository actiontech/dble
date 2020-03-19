/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
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
        FIELDS[i++].setPacketId(++packetId);

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

        SystemConfig sysConfig = DbleServer.getInstance().getConfig().getSystem();

        List<String> paramValues = new ArrayList<>();
        paramValues.add(sysConfig.getBindIp() + "");
        paramValues.add(sysConfig.getServerPort() + "");
        paramValues.add(sysConfig.getManagerPort() + "");
        paramValues.add(sysConfig.getProcessors() + "");
        paramValues.add(sysConfig.getBackendProcessors() + "");
        paramValues.add(sysConfig.getProcessorExecutor() + "");
        paramValues.add(sysConfig.getBackendProcessorExecutor() + "");
        paramValues.add(sysConfig.getComplexExecutor() + "");
        paramValues.add(sysConfig.getWriteToBackendExecutor() + "");
        paramValues.add(sysConfig.getFakeMySQLVersion());
        paramValues.add(sysConfig.getSequnceHandlerType() > 4 || sysConfig.getSequnceHandlerType() < 1 ? "Incorrect Sequence Type" : SEQUENCES[sysConfig.getSequnceHandlerType()]);
        paramValues.add(sysConfig.getServerBacklog() + "");
        paramValues.add(sysConfig.getServerNodeId() + "");
        paramValues.add(sysConfig.getShowBinlogStatusTimeout() + "ms");
        paramValues.add(sysConfig.getMaxCon() + "");
        paramValues.add(sysConfig.getUseCompression() + "");
        paramValues.add(sysConfig.getUsingAIO() + "");
        paramValues.add(sysConfig.getUseThreadUsageStat() + "");
        paramValues.add(sysConfig.getUsePerformanceMode() + "");
        paramValues.add(sysConfig.getUseCostTimeStat() + "");
        paramValues.add(sysConfig.getMaxCostStatSize() + "");
        paramValues.add(sysConfig.getCostSamplePercent() + "");
        paramValues.add(sysConfig.getCharset() + "");
        paramValues.add(sysConfig.getMaxPacketSize() / 1024 / 1024 + "M");
        paramValues.add(sysConfig.getTxIsolation() > 4 || sysConfig.getTxIsolation() < 1 ? "Incorrect isolation" : ISOLATION_LEVELS[sysConfig.getTxIsolation()]);
        paramValues.add(sysConfig.getAutocommit() + "");
        paramValues.add(sysConfig.getCheckTableConsistency() + "");
        paramValues.add(sysConfig.getCheckTableConsistencyPeriod() + "ms");
        paramValues.add(sysConfig.getDataNodeIdleCheckPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getDataNodeHeartbeatPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getProcessorCheckPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getIdleTimeout() / 1000 / 60 + " Minutes");
        paramValues.add(sysConfig.getSqlExecuteTimeout() + " Seconds");
        paramValues.add(sysConfig.getRecordTxn() + "");
        paramValues.add(sysConfig.getTransactionLogBaseDir());
        paramValues.add(sysConfig.getTransactionLogBaseName());
        paramValues.add(sysConfig.getTransactionRatateSize() + "M");
        paramValues.add(sysConfig.getXaRecoveryLogBaseDir());
        paramValues.add(sysConfig.getXaRecoveryLogBaseName());
        paramValues.add(sysConfig.getXaSessionCheckPeriod() + "ms");
        paramValues.add(sysConfig.getXaLogCleanPeriod() + "ms");
        paramValues.add(sysConfig.getXaRetryCount() + "");
        paramValues.add(sysConfig.isUseJoinStrategy() + "");
        paramValues.add(sysConfig.getNestLoopConnSize() + "");
        paramValues.add(sysConfig.getNestLoopRowsSize() + "");
        paramValues.add(sysConfig.getOtherMemSize() + "M");
        paramValues.add(sysConfig.getOrderMemSize() + "M");
        paramValues.add(sysConfig.getJoinMemSize() + "M");
        paramValues.add(sysConfig.getBufferPoolChunkSize() + "B");
        paramValues.add(sysConfig.getBufferPoolPageSize() + "B");
        paramValues.add(sysConfig.getBufferPoolPageNumber() + "");
        paramValues.add(sysConfig.getMappedFileSize() + "");
        paramValues.add(sysConfig.getUseSqlStat() + "");
        paramValues.add(sysConfig.getSqlRecordCount() + "");
        paramValues.add(sysConfig.getMaxResultSet() + "B");
        paramValues.add(sysConfig.getBufferUsagePercent() + "%");
        paramValues.add(sysConfig.getClearBigSqLResultSetMapMs() + "ms");
        paramValues.add(sysConfig.getFrontSocketSoRcvbuf() + "B");
        paramValues.add(sysConfig.getFrontSocketSoSndbuf() + "B");
        paramValues.add(sysConfig.getFrontSocketNoDelay() + "");
        paramValues.add(sysConfig.getBackSocketSoRcvbuf() + "B");
        paramValues.add(sysConfig.getBackSocketSoSndbuf() + "B");
        paramValues.add(sysConfig.getBackSocketNoDelay() + "");
        paramValues.add(sysConfig.getViewPersistenceConfBaseDir());
        paramValues.add(sysConfig.getViewPersistenceConfBaseName());
        paramValues.add(sysConfig.getJoinQueueSize() + "");
        paramValues.add(sysConfig.getMergeQueueSize() + "");
        paramValues.add(sysConfig.getOrderByQueueSize() + "");
        paramValues.add(sysConfig.getEnableSlowLog() + "");
        paramValues.add(sysConfig.getSlowLogBaseDir() + "");
        paramValues.add(sysConfig.getSlowLogBaseName() + "");
        paramValues.add(sysConfig.getFlushSlowLogPeriod() + "s");
        paramValues.add(sysConfig.getFlushSlowLogSize() + "");
        paramValues.add(sysConfig.getSqlSlowTime() + "ms");
        paramValues.add(sysConfig.getMaxCharsPerColumn() + "");
        paramValues.add(sysConfig.getMaxRowSizeToFile() + "");
        paramValues.add(sysConfig.isUseOuterHa() + "");


        for (int i = 0; i < PARAM_NAMES.length; i++) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(PARAM_NAMES[i], c.getCharset().getResults()));
            row.add(StringUtil.encode(paramValues.get(i), c.getCharset().getResults()));
            row.add(StringUtil.encode(PARAM_DESCRIPTION[i], c.getCharset().getResults()));
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

    private static final String[] PARAM_NAMES = {
            "bindIp",
            "serverPort",
            "managerPort",
            "processors",
            "backendProcessors",
            "processorExecutor",
            "backendProcessorExecutor",
            "complexExecutor",
            "writeToBackendExecutor",
            "fakeMySQLVersion",
            "sequnceHandlerType",
            "serverBacklog",
            "serverNodeId",
            "showBinlogStatusTimeout",
            "maxCon",
            "useCompression",
            "usingAIO",
            "useThreadUsageStat",
            "usePerformanceMode",
            "useCostTimeStat",
            "maxCostStatSize",
            "costSamplePercent",
            "charset",
            "maxPacketSize",
            "txIsolation",
            "autocommit",
            "checkTableConsistency",
            "checkTableConsistencyPeriod",
            "dataNodeIdleCheckPeriod",
            "dataNodeHeartbeatPeriod",
            "processorCheckPeriod",
            "idleTimeout",
            "sqlExecuteTimeout",
            "recordTxn",
            "transactionLogBaseDir",
            "transactionLogBaseName",
            "transactionRatateSize",
            "xaRecoveryLogBaseDir",
            "xaRecoveryLogBaseName",
            "xaSessionCheckPeriod",
            "xaLogCleanPeriod",
            "xaRetryCount",
            "useJoinStrategy",
            "nestLoopConnSize",
            "nestLoopRowsSize",
            "otherMemSize",
            "orderMemSize",
            "joinMemSize",
            "bufferPoolChunkSize",
            "bufferPoolPageSize",
            "bufferPoolPageNumber",
            "mappedFileSize",
            "useSqlStat",
            "sqlRecordCount",
            "maxResultSet",
            "bufferUsagePercent",
            "clearBigSqLResultSetMapMs",
            "frontSocketSoRcvbuf",
            "frontSocketSoSndbuf",
            "frontSocketNoDelay",
            "backSocketSoRcvbuf",
            "backSocketSoSndbuf",
            "backSocketNoDelay",
            "viewPersistenceConfBaseDir",
            "viewPersistenceConfBaseName",
            "joinQueueSize",
            "mergeQueueSize",
            "orderByQueueSize",
            "enableSlowLog",
            "slowLogBaseDir",
            "slowLogBaseName",
            "flushSlowLogPeriod",
            "flushSlowLogSize",
            "sqlSlowTime",
            "maxCharsPerColumn",
            "maxRowSizeToFile",
            "useOuterHa",
    };

    private static final String[] PARAM_DESCRIPTION = {
            "The host where the server is running. The default is 0.0.0.0",
            "User connection port. The default number is 8066",
            "Manager connection port. The default number is 9066",
            "The size of frontend NIOProcessor, the default is the number of processors available to the Java virtual machine",
            "The size of backend NIOProcessor, the default is the number of processors available to the Java virtual machine",
            "The size of fixed thread pool named of frontend businessExecutor,the default is the number of processors available to the Java virtual machine * 2",
            "The size of fixed thread pool named of backend businessExecutor,the default is the number of processors available to the Java virtual machine * 2",
            "The size of fixed thread pool named of writeToBackendExecutor,the default is the number of processors available to the Java virtual machine * 2",
            "The executor for complex query.The default value is min(8,processorExecutor)",
            "MySQL Version showed in Client",
            "Global Sequence Type. The default is Local TimeStamp(like Snowflake)",
            "The NIO/AIO reactor backlog,the max of create connection request at one time.The default value is 2048",
            "ServerNodeId used to create xa transaction",
            "The time out from show @@binlog.status.The default value is 60000ms",
            "The number of max connections the server allowed ",
            "Whether the Compression is enable,The default number is 0 ",
            "Whether the AIO is enable, The default number is 0(use NIO instead)",
            "Whether the thread usage statistics function is enabled.The default value is 0",
            "Whether use the performance mode is enabled.The default value is 0",
            "Whether the cost time of query can be track by Btrace.The default value is 0",
            "The max cost total percentage.The default value is 100",
            "The percentage of cost sample.The default value is 1",
            "The initially charset of connection. The default is utf8mb4",
            "The maximum size of one packet. The default is 4MB.",
            "The initially isolation level of the front end connection. The default is REPEATABLE_READ",
            "The initially autocommit value.The default value is 1",
            "Whether the consistency tableStructure check is enabled.The default value is 0",
            "The period of consistency tableStructure check .The default value is 30*60*1000",
            "The period between the heartbeat jobs for checking the health of all idle connections. The default is 300 seconds",
            "The period between the heartbeat jobs for checking the health of all write/read data sources. The default is 10 seconds",
            "The period between the jobs for cleaning the closed or overtime connections. The default is 1 second",
            "The max allowed time of idle connection. The connection will be closed if it is timed out after last read/write/heartbeat.The default is 30 minutes",
            "The max query executing time.If time out,the connection will be closed. The default is 300 seconds",
            "Whether the transaction be recorded as a file,The default value is 0",
            "The directory of the transaction record file,The default value is ./txlogs",
            "The name of the transaction record file.The default value is server-tx",
            "The max size of the transaction record file.The default value is 16M",
            "The directory of the xa transaction record file,The default value is ./tmlogs",
            "The name of the xa transaction record file.The default value is tmlog",
            "The xa transaction status check period.The default value is 1000ms",
            "The xa log clear period.The default value is 1000ms",
            "Indicates the number of background retries if the xa failed to commit/rollback.The default value is 0, retry infinitely",
            "Whether nest loop join is enabled.The default value is false",
            "The nest loop temporary tables block number.The default value is 4",
            "The nest loop temporary tables rows for every block.The default value is 2000",
            "The additional size of memory can be used in a complex query.The default size is 4M",
            "The additional size of memory can be used in a complex query order.The default size is 4M",
            "The additional size of memory can be used in a complex query join.The default size is 4M",
            "The chunk size of memory bufferPool. The min direct memory used for allocating",
            "The page size of memory bufferPool. The max direct memory used for allocating",
            "The page number of memory bufferPool. The All bufferPool size is PageNumber * PageSize",
            "The Memory linked file size,whan complex query resultSet is too large the Memory will be turned to file temporary",
            "Whether the SQL statistics function is enable or not.The default value is 1",
            "The slow SQL statistics limit,if the slow SQL record is large than the size,the record will be clear.The default value is 10",
            "The large resultSet SQL standard.The default value is 512*1024B",
            "Large result set cleanup trigger percentage.The default value is 80",
            "The period for clear the large resultSet SQL statistics.The default value is 6000000ms",
            "The buffer size of frontend receive socket.The default value is 1024*1024",
            "The buffer size of frontend send socket.The default value is 1024*1024*4",
            "The frontend nagle is disabled.The default value is 1",
            "The buffer size of backend receive socket.The default value is 1024*1024*4",
            "The buffer size of backend send socket.The default value is 1024*1024",
            "The backend nagle is disabled.The default value is 1",
            "The directory of the view record file,The default value is ./viewConf",
            "The name of the view record file.The default value is viewJson",
            "Size of join queue,Avoid using too much memory",
            "Size of merge queue,Avoid using too much memory",
            "Size of order by queue,Avoid using too much memory",
            "Enable Slow Query Log",
            "The directory of slow query log,The default value is ./slowlogs",
            "The name of the slow query log.The default value is slow-query",
            "The period for flushing log to disk, the default is 1 second",
            "The max size for flushing log to disk, the default is 1000 ",
            "The threshold of Slow Query, the default is 100ms",
            "The maximum number of characters allowed for per column when load data.The default value is 65535",
            "The maximum row size,if over this value,row data will be saved to file when load data.The default value is 10000",
            "Whether use outer ha component ",
    };

    private static final String[] ISOLATION_LEVELS = {"", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE"};
    private static final String[] SEQUENCES = {"", "Offset-Step stored in MySQL", "Local TimeStamp(like Snowflake)", "ZooKeeper/Local TimeStamp(like Snowflake)", "Offset-Step stored in ZooKeeper"};
}
