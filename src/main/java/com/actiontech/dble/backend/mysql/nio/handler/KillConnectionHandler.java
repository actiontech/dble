/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author mycat
 */
public class KillConnectionHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillConnectionHandler.class);

    private final BackendConnection toKilled;
    private final NonBlockingSession session;

    public KillConnectionHandler(BackendConnection toKilled,
                                 NonBlockingSession session) {
        this.toKilled = toKilled;
        this.session = session;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        MySQLResponseService service = (MySQLResponseService) conn.getService();
        service.setResponseHandler(this);
        service.setSession(session);
        service.sendQueryCmd(("KILL " + toKilled.getThreadId()), session.getShardingService().getCharset());
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + toKilled.toString() + " failed:" + e.getMessage(), null);
        toKilled.close("exception:" + e.toString());
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("kill connection success connection id:" +
                    toKilled.getThreadId());
        }
        ((MySQLResponseService) service).getConnection().release();
        toKilled.close("killed");

    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": field's eof");
        service.getConnection().close("close unexpected packet of killConnection");
        toKilled.close("killed");
    }

    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg;
        try {
            msg = new String(err.getMessage(), CharsetUtil.getJavaCharset(service.getConnection().getCharsetName().getResults()));
        } catch (UnsupportedEncodingException e) {
            msg = new String(err.getMessage());
        }
        LOGGER.info("kill backend connection " + toKilled + " failed: " + msg + " con:" + service);
        AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + service.toString() + " failed: " + msg, null);
        ((MySQLResponseService) service).release();
        toKilled.close("exception:" + msg);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        return false;
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + service.toString() + " failed: connectionClosed", null);
        toKilled.close("exception:" + reason);
    }
}
