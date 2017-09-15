/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, LoadDataResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);

    private final RouteResultsetNode node;
    private final RouteResultset rrs;
    protected final NonBlockingSession session;

    // only one thread access at one time no need lock
    protected volatile byte packetId;
    protected volatile ByteBuffer buffer;
    private long startTime;
    private long netInBytes;
    protected long netOutBytes;
    protected long selectRows;

    private String priamaryKeyTable = null;
    private int primaryKeyIndex = -1;

    private boolean prepared;
    private int fieldCount;
    private List<FieldPacket> fieldPackets = new ArrayList<>();


    private volatile boolean waitingResponse;

    public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];
        if (node == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
    }


    public void execute() throws Exception {
        startTime = System.currentTimeMillis();
        ServerConnection sc = session.getSource();
        waitingResponse = true;
        this.packetId = 0;
        final BackendConnection conn = session.getTarget(node);
        node.setRunOnSlave(rrs.getRunOnSlave());

        if (session.tryExistsCon(conn, node)) {
            execute(conn);
        } else {
            // create new connection
            node.setRunOnSlave(rrs.getRunOnSlave());

            ServerConfig conf = DbleServer.getInstance().getConfig();
            PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), sc.isAutocommit(), node, this, node);
        }

    }

    private void execute(BackendConnection conn) {
        if (session.closed()) {
            waitingResponse = false;
            session.clearResources(true);
            return;
        }
        conn.setResponseHandler(this);
        boolean isAutocommit = session.getSource().isAutocommit() && !session.getSource().isTxstart();
        if (!isAutocommit && node.isModifySQL()) {
            TxnLogHelper.putTxnLog(session.getSource(), node.getStatement());
        }
        conn.execute(node, session.getSource(), isAutocommit);
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        session.bindConnection(node, conn);
        execute(conn);

    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        session.handleSpecial(rrs, session.getSource().getSchema(), true);
        recycleResources();
        session.getSource().close(e.getMessage());
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        err.setPacketId(++packetId);
        backConnectionErr(err, conn);
    }

    private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
        ServerConnection source = session.getSource();
        String errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();

        String errmgs = " errno:" + errPkg.getErrno() + " " + new String(errPkg.getMessage());
        LOGGER.warn("execute  sql err :" + errmgs + " con:" + conn +
                " frontend host:" + errHost + "/" + errPort + "/" + errUser);

        session.releaseConnectionIfSafe(conn, false);

        source.setTxInterrupt(errmgs);
        session.handleSpecial(rrs, session.getSource().getSchema(), false);

        /**
         *
         * BUG:
         * 1. MysqlClient:  SELECT 9223372036854775807 + 1;
         * 2. MyCatServer:  ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
         * 3. MysqlClient: ERROR 2013 (HY000): Lost connection to MySQL server during query
         * because of  pakcetId != 1
         * Fixed:
         * 1. MysqlClient:  SELECT 9223372036854775807 + 1;
         * 2. MyCatServer:  ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
         * 3. MysqlClient: ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
         *
         */
        //
        if (waitingResponse) {
            errPkg.setPacketId(1);
            errPkg.write(source);
            waitingResponse = false;
        }
        recycleResources();
    }


    /**
     * insert/update/delete
     * <p>
     * okResponse():
     * read data, make an OKPacket, write to writeQueue in FrontendConnection by ok.write(source)
     */
    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        //
        this.netOutBytes += data.length;

        boolean executeResponse = conn.syncAndExcute();
        if (executeResponse) {
            session.handleSpecial(rrs, session.getSource().getSchema(), true);
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            if (rrs.isLoadData()) {
                byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
                ok.setPacketId(++lastPackId); // OK_PACKET
                source.getLoadDataInfileHandler().clear();

            } else {
                ok.setPacketId(++packetId); // OK_PACKET
            }
            ok.setServerStatus(source.isAutocommit() ? 2 : 1);
            source.setLastInsertId(ok.getInsertId());
            //handleSpecial
            session.releaseConnectionIfSafe(conn, false);
            ok.write(source);
            waitingResponse = false;
        }
    }


    /**
     * select
     * <p>
     * write EOF to Queue
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {

        this.netOutBytes += eof.length;

        ServerConnection source = session.getSource();
        // if it's call statement,it will not release connection
        if (!rrs.isCallStatement() || rrs.getProcedure().isResultSimpleValue()) {
            session.releaseConnectionIfSafe(conn, false);
        }

        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, allocBuffer());
        int resultSize = source.getWriteQueue().size() * DbleServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
        resultSize = resultSize + buffer.position();
        source.write(buffer);
        waitingResponse = false;

        if (DbleServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
            if (rrs.getStatement() != null) {
                netInBytes += rrs.getStatement().getBytes().length;
            }
            QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(), rrs.getStatement(), selectRows,
                    netInBytes, netOutBytes, startTime, System.currentTimeMillis(), resultSize);
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    private void recycleResources() {

        ByteBuffer buf = buffer;
        if (buf != null) {
            session.getSource().recycle(buffer);
            buffer = null;
        }
    }

    /**
     * lazy create ByteBuffer only when needed
     *
     * @return
     */
    protected ByteBuffer allocBuffer() {
        if (buffer == null) {
            buffer = session.getSource().allocate();
        }
        return buffer;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsnull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
        }

        String primaryKey = null;
        if (rrs.hasPrimaryKeyToCache()) {
            String[] items = rrs.getPrimaryKeyItems();
            priamaryKeyTable = items[0];
            primaryKey = items[1];
        }

        header[3] = ++packetId;

        ServerConnection source = session.getSource();
        buffer = source.writeToBuffer(header, allocBuffer());
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            field[3] = ++packetId;

            // save field
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            fieldPackets.add(fieldPk);

            // find primary key index
            if (primaryKey != null && primaryKeyIndex == -1) {
                String fieldName = new String(fieldPk.getName());
                if (primaryKey.equalsIgnoreCase(fieldName)) {
                    primaryKeyIndex = i;
                }
            }

            buffer = source.writeToBuffer(field, buffer);
        }

        fieldCount = fieldPackets.size();

        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {

        this.netOutBytes += row.length;
        this.selectRows++;
        row[3] = ++packetId;

        RowDataPacket rowDataPk = null;
        // cache primaryKey-> dataNode
        if (primaryKeyIndex != -1) {
            rowDataPk = new RowDataPacket(fieldCount);
            rowDataPk.read(row);
            String primaryKey = new String(rowDataPk.fieldValues.get(primaryKeyIndex));
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            LayerCachePool pool = DbleServer.getInstance().getRouterService().getTableId2DataNodeCache();
            if (pool != null) {
                pool.putIfAbsent(priamaryKeyTable, primaryKey, rNode.getName());
            }
        }

        if (prepared) {
            if (rowDataPk == null) {
                rowDataPk = new RowDataPacket(fieldCount);
                rowDataPk.read(row);
            }
            BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
            binRowDataPk.read(fieldPackets, rowDataPk);
            binRowDataPk.setPacketId(rowDataPk.getPacketId());
            buffer = binRowDataPk.write(buffer, session.getSource(), true);
        } else {
            buffer = session.getSource().writeToBuffer(row, allocBuffer());
            //session.getSource().write(row);
        }
        return false;
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(++packetId);
        err.setErrno(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        this.backConnectionErr(err, conn);
        session.getSource().close(reason);
    }

    public void clearResources() {

    }

    @Override
    public void requestDataResponse(byte[] data, BackendConnection conn) {
        LoadDataUtil.requestFileDataResponse(data, conn);
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    @Override
    public String toString() {
        return "SingleNodeHandler [node=" + node + ", packetId=" + packetId + "]";
    }


    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {

    }


    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {

    }

}
