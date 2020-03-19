/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * for execute Sql,transform the response data to next handler
 */
public class BaseSelectHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSelectHandler.class);

    private final boolean autocommit;
    private volatile int fieldCounts = -1;


    private RouteResultsetNode rrss;


    public BaseSelectHandler(long id, RouteResultsetNode rrss, boolean autocommit, NonBlockingSession session) {
        super(id, session);
        this.rrss = rrss;
        this.autocommit = autocommit;
    }

    public MySQLConnection initConnection() throws Exception {
        if (session.closed()) {
            return null;
        }

        MySQLConnection exeConn = (MySQLConnection) session.getTarget(rrss);
        if (session.tryExistsCon(exeConn, rrss)) {
            exeConn.setRowDataFlowing(true);
            return exeConn;
        } else {
            PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(rrss.getName());
            //autocommit is session.getWriteSource().isAutocommit() && !session.getWriteSource().isTxStart()
            final BackendConnection newConn = dn.getConnection(dn.getDatabase(), autocommit, rrss.getRunOnSlave(), rrss);
            session.bindConnection(rrss, newConn);
            newConn.setResponseHandler(this);
            ((MySQLConnection) newConn).setRowDataFlowing(true);
            return (MySQLConnection) newConn;
        }
    }

    public void execute(MySQLConnection conn) {
        if (session.closed()) {
            conn.setRowDataFlowing(false);
            session.clearResources(true);
            return;
        }
        conn.setSession(session);
        if (conn.isClosed()) {
            conn.setRowDataFlowing(false);
            session.onQueryError("failed or cancelled by other thread".getBytes());
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(conn.toString() + " send sql:" + rrss.getStatement());
        }
        conn.execute(rrss, session.getSource(), autocommit);
    }

    public RouteResultsetNode getRrss() {
        return rrss;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        conn.syncAndExecute();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        session.setHandlerEnd(this); //base start receive
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(conn.toString() + "'s field is reached.");
        }
        if (terminate.get()) {
            return;
        }
        if (fieldCounts == -1) {
            fieldCounts = fields.size();
        }
        List<FieldPacket> fieldPackets = new ArrayList<>();

        for (byte[] field1 : fields) {
            FieldPacket field = new FieldPacket();
            field.read(field1);
            fieldPackets.add(field);
        }
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        RowDataPacket rp = new RowDataPacket(fieldCounts);
        rp.read(row);
        nextHandler.rowResponse(null, rp, this.isLeft, conn);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(conn.toString() + " 's rowEof is reached.");
        }
        if (this.terminate.get()) {
            return;
        }
        nextHandler.rowEofResponse(data, this.isLeft, conn);
    }

    /**
     * 1. if some connection's thread status is await. 2. if some connection's
     * thread status is running.
     */
    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        if (terminate.get())
            return;
        String errMsg;
        if (e instanceof MySQLOutPutException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else if (e instanceof NullPointerException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else {
            LOGGER.warn("Backend connect Error, Connection info:" + conn, e);
            errMsg = "Backend connect Error, Connection{DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        }
        session.onQueryError(errMsg.getBytes());
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (terminate.get())
            return;
        LOGGER.warn(conn.toString() + "|connectionClose()|" + reason);
        reason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        session.onQueryError(reason.getBytes());
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg;
        try {
            errMsg = new String(errPacket.getMessage(), CharsetUtil.getJavaCharset(conn.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            errMsg = "UnsupportedEncodingException:" + conn.getCharset();
        }
        LOGGER.info(conn.toString() + errMsg);
        if (terminate.get())
            return;
        session.onQueryError(errMsg.getBytes());
    }

    @Override
    protected void onTerminate() {
        if (autocommit && !session.getSource().isLocked()) {
            this.session.releaseConnection(rrss, LOGGER.isDebugEnabled(), false);
        } else {
            //the connection should wait until the connection running finish
            this.session.waitFinishConnection(rrss);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.BASESEL;
    }

}
