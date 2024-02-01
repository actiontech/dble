package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.nio.handler.PreparedResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
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
    private volatile byte[] ok;
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
        byte type = data[4];
        if (type == OkPacket.FIELD_COUNT) {
            boolean executeResponse = service.syncAndExecute();
            if (executeResponse) {
                final int fieldCount = (int) ByteUtil.readLength(data, 9);
                final int paramCount = (int) ByteUtil.readLength(data, 11);
                if (fieldCount > 0) {
                    fields = new ArrayList<>(fieldCount + 1);
                    resultStatus = PREPARED_FIELD;
                }
                if (paramCount > 0) {
                    params = new ArrayList<>(paramCount + 1);
                    resultStatus = PREPARED_PARAM;
                }
                if (fieldCount == 0 && paramCount == 0) {
                    // handle ok packet
                    handleOkPacket(data);
                    return;
                }
                ok = data;
            }
        } else if (type == ErrorPacket.FIELD_COUNT) {
            if (resultStatus == PREPARED_FIELD) {
                fields.add(data);
                // handle field eof
                handleOkPacket(ok);
            } else {
                params.add(data);
                if (fields != null) {
                    resultStatus = PREPARED_FIELD;
                } else {
                    // handle param eof
                    handleOkPacket(ok);
                }
            }
        } else if (type == EOFPacket.FIELD_COUNT) {
            if (resultStatus == PREPARED_FIELD) {
                fields.add(data);
                // handle field eof
                handleOkPacket(ok);
            } else {
                params.add(data);
                if (fields != null) {
                    resultStatus = PREPARED_FIELD;
                } else {
                    // handle param eof
                    handleOkPacket(ok);
                }
            }
        } else {
            data(data);
        }
    }


    public void data(byte[] data) {
        if (resultStatus == PREPARED_FIELD) {
            fields.add(data);
        } else {
            params.add(data);
        }
    }

    private void handleOkPacket(byte[] okPacket) {
        ResponseHandler respHand = service.getResponseHandler();
        if (respHand instanceof PreparedResponseHandler) {
            ((PreparedResponseHandler) respHand).preparedOkResponse(okPacket, fields, params, service);
            ok = null;
            params = null;
            fields = null;
            resultStatus = PREPARED_FIELD;
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
