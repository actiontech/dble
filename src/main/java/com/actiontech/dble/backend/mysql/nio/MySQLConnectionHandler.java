/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.handler.BackendAsyncHandler;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * life cycle: from connection establish to close <br/>
 *
 * @author mycat
 */
public class MySQLConnectionHandler extends BackendAsyncHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnectionHandler.class);
    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;

    private final MySQLConnection source;
    private volatile int resultStatus;
    private volatile byte[] header;
    private volatile List<byte[]> fields;

    /**
     * life cycle: one SQL execution
     */
    private volatile ResponseHandler responseHandler;

    private volatile NonBlockingSession session;

    public MySQLConnectionHandler(MySQLConnection source) {
        this.source = source;
        this.resultStatus = RESULT_STATUS_INIT;
    }

    @Override
    public void handle(byte[] data) {
        if (session != null) {
            if (session.isKilled()) return;
            session.setBackendResponseTime(source);
        }
        if (source.isComplexQuery()) {
            offerData(data, DbleServer.getInstance().getComplexQueryExecutor());
        } else if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            offerData(data);
        } else {
            offerData(data, DbleServer.getInstance().getBackendBusinessExecutor());
        }
    }

    @Override
    protected void offerDataError() {
        resultStatus = RESULT_STATUS_INIT;
        throw new RuntimeException("offer data error!");
    }

    @Override
    protected void handleData(byte[] data) {
        if (source.isClosed()) {
            if (data != null && data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                parseErrorPacket(data, "connection close");
            }
            return;
        }
        switch (resultStatus) {
            case RESULT_STATUS_INIT:
                if (session != null) {
                    session.startExecuteBackend(source.getId());
                }
                switch (data[4]) {
                    case OkPacket.FIELD_COUNT:
                        handleOkPacket(data);
                        break;
                    case ErrorPacket.FIELD_COUNT:
                        handleErrorPacket(data);
                        break;
                    case RequestFilePacket.FIELD_COUNT:
                        handleRequestPacket(data);
                        break;
                    default:
                        resultStatus = RESULT_STATUS_HEADER;
                        header = data;
                        fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
                }
                break;
            case RESULT_STATUS_HEADER:
                switch (data[4]) {
                    case ErrorPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_INIT;
                        handleErrorPacket(data);
                        break;
                    case EOFPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_FIELD_EOF;
                        handleFieldEofPacket(data);
                        break;
                    default:
                        fields.add(data);
                }
                break;
            case RESULT_STATUS_FIELD_EOF:
                switch (data[4]) {
                    case ErrorPacket.FIELD_COUNT:
                        resultStatus = RESULT_STATUS_INIT;
                        handleErrorPacket(data);
                        break;
                    case EOFPacket.FIELD_COUNT:
                        if (data.length > MySQLPacket.MAX_EOF_SIZE) {
                            handleRowPacket(data);
                        } else {
                            resultStatus = RESULT_STATUS_INIT;
                            handleRowEofPacket(data);
                        }
                        break;
                    default:
                        handleRowPacket(data);
                }
                break;
            default:
                throw new RuntimeException("unknown status!");
        }
    }

    public void setResponseHandler(ResponseHandler responseHandler) {
        // logger.info("set response handler "+responseHandler);
        // if (this.responseHandler != null && responseHandler != null) {
        // throw new RuntimeException("reset agani!");
        // }
        this.responseHandler = responseHandler;
    }

    public void setSession(NonBlockingSession session) {
        this.session = session;
    }

    /**
     * execute OK Packet
     */
    private void handleOkPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.okResponse(data, source);
        }
    }

    /**
     * execute ERROR packet
     */
    private void handleErrorPacket(byte[] data) {
        final ResponseHandler respHand = responseHandler;
        this.source.setExecuting(false);
        this.source.setRowDataFlowing(false);
        this.source.signal();
        if (respHand != null) {
            respHand.errorResponse(data, source);
        } else {
            try {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.read(data);
                String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
                LOGGER.warn("no handler process the execute sql err,just close it, sql error:{},back con:{}", errMsg, source);
                if (session != null) {
                    LOGGER.warn("no handler process the execute sql err,front conn {}", session.getSource());
                }

            } catch (RuntimeException e) {
                LOGGER.info("error handle error-packet", e);
            }
            closeNoHandler();
        }
    }

    /**
     * execute load request Packet:load data file
     */
    private void handleRequestPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null && respHand instanceof LoadDataResponseHandler) {
            ((LoadDataResponseHandler) respHand).requestDataResponse(data, source);
        } else {
            closeNoHandler();
        }
    }

    /**
     * execute FieldEof Packet
     */
    private void handleFieldEofPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        this.source.setRowDataFlowing(true);
        if (respHand != null) {
            respHand.fieldEofResponse(header, fields, null, data, false, source);
        } else {
            closeNoHandler();
        }
    }

    /**
     * execute Row Packet
     */
    private void handleRowPacket(byte[] data) {
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            respHand.rowResponse(data, null, false, source);
        } else {
            closeNoHandler();

        }
    }

    private void closeNoHandler() {
        if (!source.isClosed()) {
            source.close("no handler");
            LOGGER.info("no handler bind in this con " + this + " client:" + source);
        }
    }

    /**
     * execute RowEof Packet
     */
    private void handleRowEofPacket(byte[] data) {
        if (session != null && !source.isTesting() && this.source.getLogResponse().compareAndSet(false, true)) {
            session.setBackendResponseEndTime(this.source);
        }
        this.source.getLogResponse().set(false);
        ResponseHandler respHand = responseHandler;
        if (respHand != null) {
            this.source.backendSpecialCleanUp();
            respHand.rowEofResponse(data, false, source);
        } else {
            closeNoHandler();
        }
    }

    @Override
    protected void handleDataError(Exception e) {
        LOGGER.info(this.source.toString() + " handle data error:", e);
        while (dataQueue.size() > 0) {
            clearTaskQueue();
            // clear all data from the client
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
        resultStatus = RESULT_STATUS_INIT;
        this.source.close("handle data error:" + e.getMessage());
    }

    private void clearTaskQueue() {
        byte[] data;
        while ((data = dataQueue.poll()) != null) {
            if (data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                parseErrorPacket(data, "cleanup");
            }

        }
    }

    public void parseErrorPacket(byte[] data, String reason) {
        try {
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.read(data);
            String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
            LOGGER.warn("drop the unprocessed error packet, reason:{},packet content:{},back service:{},", reason, errMsg, source);

        } catch (RuntimeException e) {
            LOGGER.info("error drop error-packet", e);
        }
    }
}
