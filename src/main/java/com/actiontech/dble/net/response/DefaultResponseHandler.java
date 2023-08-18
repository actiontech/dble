/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.response;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.btrace.provider.IODelayProvider;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * for select and dml response packet
 *
 * @author collapsar
 */
public class DefaultResponseHandler implements ProtocolResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResponseHandler.class);

    private volatile int status = INITIAL;
    protected volatile byte[] header;
    protected volatile List<byte[]> fields;

    protected MySQLResponseService service;

    public DefaultResponseHandler(MySQLResponseService service) {
        this.service = service;
    }

    @Override
    public void ok(byte[] data) {
        if (status == INITIAL) {
            if (service.getSession() != null) {
                service.getSession().trace(t -> t.startExecuteBackend());
            }
            ResponseHandler respHand = service.getResponseHandler();
            if (respHand != null) {
                Optional.ofNullable(service.getOriginSession()).ifPresent(p -> {
                    OkPacket ok = new OkPacket();
                    ok.read(data);
                    p.trace(t -> t.setBackendSqlSetRows(service, ok.getAffectedRows()));
                });
                respHand.okResponse(data, service);
            }
        } else if (status == FIELD) {
            fields.add(data);
        } else {
            handleRowPacket(data);
        }
    }

    @Override
    public void error(byte[] data) {
        final ResponseHandler respHand = service.getResponseHandler();
        service.setExecuting(false);
        beforeError();

        if (respHand != null) {
            if (service.getOriginSession() != null)
                service.getOriginSession().trace(t -> t.setBackendResponseEndTime(service));
            IODelayProvider.beforeErrorResponse(service);
            respHand.errorResponse(data, service);
        } else {
            try {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.read(data);
                String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
                LOGGER.warn("no handler process the execute sql err,just close it, sql error:{},back con:{}", errMsg, service);
                if (service.getOriginSession() != null) {
                    LOGGER.warn("no handler process the execute sql err,front conn {}", service.getOriginSession().getSource());
                }

            } catch (RuntimeException e) {
                LOGGER.info("error handle error-packet", e);
            }
            closeNoHandler();
        }
    }

    protected void beforeError() {
        if (service.getSession() != null) {
            service.getSession().trace(t -> t.startExecuteBackend());
        }
        service.releaseSignal();
        status = INITIAL;
    }

    @Override
    public void eof(byte[] eof) {
        if (status == FIELD) {
            status = ROW;
            handleFieldEofPacket(eof);
        } else if (eof.length > MySQLPacket.MAX_EOF_SIZE) {
            handleRowPacket(eof);
        } else {
            status = INITIAL;
            handleRowEofPacket(eof);
        }
    }

    @Override
    public void data(byte[] data) {
        if (status == INITIAL) {
            if (service.getSession() != null) {
                service.getSession().trace(t -> t.startExecuteBackend());
            }
            status = FIELD;
            header = data;
            fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
        } else if (status == FIELD) {
            fields.add(data);
        } else {
            handleRowPacket(data);
        }
    }

    protected void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            LOGGER.info("no handler bind in this service " + service);
            if (service.getOriginSession() != null)
                service.getOriginSession().trace(t -> t.setBackendResponseEndTime(service));
            service.getConnection().close("no handler");
        }
    }

    private void handleFieldEofPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        service.setRowDataFlowing(true);
        if (respHand != null) {
            respHand.fieldEofResponse(header, fields, null, data, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRowPacket(byte[] data) {
        //LOGGER.info("get into rowing data " + data.length);
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            if (service.getOriginSession() != null)
                service.getOriginSession().trace(t -> t.setBackendSqlAddRows(service));
            respHand.rowResponse(data, null, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleRowEofPacket(byte[] data) {
        if (service.getOriginSession() != null && !service.isTesting() && service.getLogResponse().compareAndSet(false, true)) {
            service.getOriginSession().trace(t -> t.setBackendResponseEndTime(this.service));
        }
        service.getLogResponse().set(false);
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            service.backendSpecialCleanUp();
            respHand.rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

}
