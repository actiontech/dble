package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.MysqlBackendLogicHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/7/6.
 */
public class MysqlPrepareLogicHandler extends MysqlBackendLogicHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlPrepareLogicHandler.class);

    private volatile List<byte[]> params;
    private volatile List<byte[]> fields;


    public MysqlPrepareLogicHandler(MySQLResponseService service) {
        super(service);
        resultStatus = PREPARED_OK;
    }

    public void handleInnerData(byte[] data) {
        if (service.getConnection().isClosed()) {
            if (data != null && data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                service.parseErrorPacket(data, "connection close");
            }
            return;
        }
        switch (resultStatus) {
            case PREPARED_OK:
                boolean executeResponse = service.syncAndExecute();
                if (executeResponse) {
                    // Prepare ok
                    handleOkPacket(data);
                    int fieldCount = ByteUtil.readUB2(data, 9);
                    int paramCount = ByteUtil.readUB2(data, 11);
                    if (fieldCount > 0) {
                        fields = new ArrayList<>(fieldCount);
                        resultStatus = PREPARED_FIELD;
                    }
                    if (paramCount > 0) {
                        params = new ArrayList<>(paramCount);
                        resultStatus = PREPARED_PARAM;
                    }
                }
                break;
            case PREPARED_PARAM:
                if (data[4] == EOFPacket.FIELD_COUNT) {
                    resultStatus = PREPARED_FIELD;
                    handleParamEofPacket(data);
                } else {
                    params.add(data);
                }
                break;
            case PREPARED_FIELD:
                if (data[4] == EOFPacket.FIELD_COUNT) {
                    resultStatus = RESULT_STATUS_INIT;
                    handleFieldEofPacket(data);
                } else {
                    fields.add(data);
                }
                break;
            default:
                super.handleInnerData(data);
        }
    }

    private void handleOkPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).preparedOkResponse(data, service);
        }
    }

    private void handleParamEofPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).paramEofResponse(params, data, service);
        } else {
            closeNoHandler();
        }
    }

    private void handleFieldEofPacket(byte[] data) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).fieldEofResponse(fields, data, service);
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

    @Override
    public void reset() {
        resultStatus = PREPARED_OK;
    }
}
