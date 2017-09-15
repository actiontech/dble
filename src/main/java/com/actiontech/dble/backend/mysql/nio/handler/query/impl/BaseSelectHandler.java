/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * for execute Sql,transform the response data to next handler
 */
public class BaseSelectHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(BaseSelectHandler.class);

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
            return exeConn;
        } else {
            ServerConfig conf = DbleServer.getInstance().getConfig();
            PhysicalDBNode dn = conf.getDataNodes().get(rrss.getName());
            final BackendConnection newConn = dn.getConnection(dn.getDatabase(), autocommit);
            session.bindConnection(rrss, newConn);
            return (MySQLConnection) newConn;
        }
    }

    public void execute(MySQLConnection conn) {
        if (session.closed()) {
            session.clearResources(true);
            return;
        }
        conn.setResponseHandler(this);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(conn.toString() + " send sql:" + rrss.getStatement());
        }
        if (session.closed()) {
            session.onQueryError("failed or cancelled by other thread".getBytes());
            return;
        }
        conn.execute(rrss, session.getSource(), autocommit);
    }

    public RouteResultsetNode getRrss() {
        return rrss;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        conn.syncAndExcute();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
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
        ((MySQLConnection) conn).setRunning(false);
        if (this.terminate.get())
            return;
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
        LOGGER.warn(
                conn.toString() + "|connectionError()|" + e.getMessage());
        session.onQueryError(e.getMessage().getBytes());
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ((MySQLConnection) conn).setRunning(false);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg;
        try {
            errMsg = new String(errPacket.getMessage(), CharsetUtil.getJavaCharset(conn.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            errMsg = "UnsupportedEncodingException:" + conn.getCharset();
        }
        LOGGER.warn(conn.toString() + errMsg);
        if (terminate.get())
            return;
        session.onQueryError(errMsg.getBytes());
    }

    @Override
    protected void onTerminate() {
        if (autocommit) {
            this.session.releaseConnection(rrss, LOGGER.isDebugEnabled(), false);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.BASESEL;
    }

}
