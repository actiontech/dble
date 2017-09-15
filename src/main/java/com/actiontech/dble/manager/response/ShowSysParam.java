/*
 * Copyright (C) 2016-2017 ActionTech.
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
        paramValues.add(sysConfig.getProcessors() + "");
        paramValues.add(sysConfig.getBufferPoolChunkSize() + "B");
        paramValues.add(sysConfig.getBufferPoolPageSize() + "B");
        paramValues.add(sysConfig.getBufferPoolPageNumber() + "B");
        paramValues.add(sysConfig.getProcessorExecutor() + "");
        paramValues.add(sysConfig.getSequnceHandlerType() > 4 || sysConfig.getSequnceHandlerType() < 1 ? "Incorrect Sequence Type" : SEQUENCES[sysConfig.getSequnceHandlerType()]);
        paramValues.add(sysConfig.getMaxPacketSize() / 1024 / 1024 + "M");
        paramValues.add(sysConfig.getIdleTimeout() / 1000 / 60 + " Minutes");
        paramValues.add(sysConfig.getCharset() + "");
        paramValues.add(sysConfig.getTxIsolation() > 4 || sysConfig.getTxIsolation() < 1 ? "Incorrect isolation" : ISOLATION_LEVELS[sysConfig.getTxIsolation()]);
        paramValues.add(sysConfig.getSqlExecuteTimeout() + " Seconds");
        paramValues.add(sysConfig.getProcessorCheckPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getDataNodeIdleCheckPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getDataNodeHeartbeatPeriod() / 1000 + " Seconds");
        paramValues.add(sysConfig.getBindIp() + "");
        paramValues.add(sysConfig.getServerPort() + "");
        paramValues.add(sysConfig.getManagerPort() + "");

        for (int i = 0; i < PARAMNAMES.length; i++) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(PARAMNAMES[i], c.getCharset().getResults()));
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

    private static final String[] PARAMNAMES = {
            "processors",
            "bufferPoolChunkSize",
            "bufferPoolPageSize",
            "bufferPoolPageNumber",
            "processorExecutor",
            "sequenceHandlerType",
            "maxPacketSize",
            "idleTimeout",
            "charset",
            "txIsolation",
            "sqlExecuteTimeout",
            "processorCheckPeriod",
            "dataNodeIdleCheckPeriod",
            "dataNodeHeartbeatPeriod",
            "bindIp",
            "serverPort",
            "managerPort"};

    private static final String[] PARAM_DESCRIPTION = {
            "The size of NIOProcessor, the default is the number of processors available to the Java virtual machine",
            "The chunk size of memory bufferPool. The min direct memory used for allocating",
            "The page size of memory bufferPool. The max direct memory used for allocating",
            "The page number of memory bufferPool. The bufferPool size is PageNumber * PageSize",
            "The size of fixed thread pool named of businessExecutor and the core size of cached thread pool named of complexQueryExecutor .",
            "Global Sequence Type. The default is Local TimeStamp(like Snowflake)",
            "The maximum size of one packet. The default is 16MB.",
            "The max allowed time of idle connection. The connection will be closed if it is timed out after last read/write/heartbeat.The default is 30 minutes",
            "The initially charset of connection. The default is UTF8",
            "The initially isolation level of the front end connection. The default is REPEATED_READ",
            "The max query executing time.If time out,the connection will be closed. The default is 300 seconds",
            "The period between the jobs for cleaning the closed or overtime connections. The default is 1 second",
            "The period between the heartbeat jobs for checking the health of all idle connections. The default is 300 seconds",
            "The period between the heartbeat jobs for checking the health of all write/read data sources. The default is 10 seconds",
            "The host where the server is running. The default is 0.0.0.0",
            "User connection port. The default number is 8066",
            "Manager connection port. The default number is 9066"};

    private static final String[] ISOLATION_LEVELS = {"", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATED_READ", "SERIALIZABLE"};
    private static final String[] SEQUENCES = {"", "Offset-Step stored in MySQL", "Local TimeStamp(like Snowflake)", "ZooKeeper/Local TimeStamp(like Snowflake)", "Offset-Step stored in ZooKeeper"};
}
