/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
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
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("kill connection success connection id:" +
                    toKilled.getThreadId());
        }
        ((MySQLResponseService) service).getConnection().release();
        toKilled.close("killed");

    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": field's eof");
        service.getConnection().close("close unexpected packet of killConnection");
        toKilled.close("killed");
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg;
        try {
            msg = new String(err.getMessage(), CharsetUtil.getJavaCharset(service.getCharset().getResults()));
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
                                 boolean isLeft, @NotNull AbstractService service) {
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        return false;
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + service.toString() + " failed: connectionClosed", null);
        toKilled.getBackendService().setResponseHandler(null);
        toKilled.close("exception:" + reason);
    }
}
