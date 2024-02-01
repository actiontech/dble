package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.LoadDataResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.MysqlBackendLogicHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MysqlExecuteResponseHandler extends MysqlBackendLogicHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlExecuteResponseHandler.class);

    private final boolean cursor;
    private volatile boolean okAfterEof = false;

    public MysqlExecuteResponseHandler(MySQLResponseService service, boolean cursor) {
        super(service);
        this.cursor = cursor;
    }

    public void handleInnerData(byte[] data) {
        if (service.getConnection().isClosed()) {
            if (data != null && data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                service.parseErrorPacket(data, "connection close");
            }
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
                        eof(data);
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



    private void handleErrorPacket(byte[] data) {
        final ResponseHandler respHand = service.getResponseHandler();
        service.setExecuting(false);
        service.signal();
        if (respHand != null) {
            respHand.errorResponse(data, service);
        } else {
            try {
                ErrorPacket errPkg = new ErrorPacket();
                errPkg.read(data);
                String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
                LOGGER.warn("no handler process the execute sql err,just close it, sql error:{},back con:{}", errMsg, service);
                if (service.getSession() != null) {
                    LOGGER.warn("no handler process the execute sql err,front conn {}", service.getSession().getSource());
                }

            } catch (RuntimeException e) {
                LOGGER.info("error handle error-packet", e);
            }
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
        service.getLogResponse().set(false);
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            service.backendSpecialCleanUp();
            respHand.rowEofResponse(data, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleOkPacket(byte[] data) {
        // if prepared statement doesn't keep cursor on, the response contains additional ok packet
        if (okAfterEof) {
            resultStatus = RESULT_STATUS_HEADER;
            header = data;
            fields = new ArrayList<>((int) ByteUtil.readLength(data, 4));
        } else {
            ok(data);
        }
    }


    private void ok(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand != null) {
            respHand.okResponse(data, service);
        }
    }


    private void eof(byte[] eof) {
        if (cursor) {
            handleFieldEofPacket(eof);
        } else {
            okAfterEof = true;
            handleBackendFieldEofPacket(eof);
        }
    }

    private void handleBackendFieldEofPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        service.setRowDataFlowing(true);
        if (respHand != null) {
            respHand.fieldEofResponse(header, fields, null, data, false, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleFieldEofPacket(byte[] eof) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).preparedExecuteResponse(header, fields, eof, service);
        } else {
            closeNoHandler();
        }
    }
}
