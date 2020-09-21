package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/7/6.
 */
public class MysqlBackendLogicHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlBackendLogicHandler.class);

    protected volatile int resultStatus = RESULT_STATUS_INIT;
    protected static final int RESULT_STATUS_INIT = 0;
    protected static final int RESULT_STATUS_HEADER = 1;
    protected static final int RESULT_STATUS_FIELD_EOF = 2;
    // prepared statement
    protected static final int PREPARED_OK = 3;
    protected static final int PREPARED_PARAM = 4;
    protected static final int PREPARED_FIELD = 5;

    protected final MySQLResponseService service;
    protected volatile byte[] header;
    protected volatile List<byte[]> fields;

    public MysqlBackendLogicHandler(MySQLResponseService service) {
        this.service = service;
    }

    protected void handleInnerData(byte[] data) {
        if (service.getConnection().isClosed()) {
            return;
        }
        switch (resultStatus) {
            case RESULT_STATUS_INIT:
                if (service.getSession() != null) {
                    service.getSession().startExecuteBackend(service.getConnection().getId());
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
                        reset();
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
                        reset();
                        handleErrorPacket(data);
                        break;
                    case EOFPacket.FIELD_COUNT:
                        if (data.length > MySQLPacket.MAX_EOF_SIZE) {
                            handleRowPacket(data);
                        } else {
                            reset();
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

    private void handleOkPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            respHand.okResponse(data, service);
        }
    }


    private void handleErrorPacket(byte[] data) {
        final ResponseHandler respHand = service.getResponseHandler();
        service.setExecuting(false);
        service.setRowDataFlowing(false);
        service.signal();
        if (respHand != null) {
            respHand.errorResponse(data, service);
        } else {
            closeNoHandler();
        }
    }

    private void closeNoHandler() {
        if (!service.getConnection().isClosed()) {
            service.getConnection().close("no handler");
            LOGGER.info("no handler bind in this con " + this + " client:" + service);
        }
    }

    private void handleRequestPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof LoadDataResponseHandler) {
            ((LoadDataResponseHandler) respHand).requestDataResponse(data, service);
        } else {
            closeNoHandler();
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
            respHand.rowResponse(data, null, false, service);
        } else {
            closeNoHandler();
        }
    }


    private void handleRowEofPacket(byte[] data) {
        if (service.getSession() != null && !service.isTesting() && service.getLogResponse().compareAndSet(false, true)) {
            service.getSession().setBackendResponseEndTime(this.service);
        }
        service.setExecuting(false);
        service.setRowDataFlowing(false);
        service.getLogResponse().set(false);
        service.signal();
        if (service.getResponseHandler() != null) {
            service.getResponseHandler().rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

    protected void reset() {
        resultStatus = RESULT_STATUS_INIT;
    }
}
