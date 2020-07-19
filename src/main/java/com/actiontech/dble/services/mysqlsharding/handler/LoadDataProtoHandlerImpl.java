package com.actiontech.dble.services.mysqlsharding.handler;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.handler.LoadDataInfileHandler;

import java.nio.ByteBuffer;


/**
 * Created by szf on 2020/7/14.
 */
public class LoadDataProtoHandlerImpl extends MySQLProtoHandlerImpl {

    private final LoadDataInfileHandler loadDataHandler;

    public LoadDataProtoHandlerImpl(LoadDataInfileHandler loadDataHandler) {
        this.loadDataHandler = loadDataHandler;
    }

    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset, boolean isSupportCompress) {
        ProtoHandlerResult result = super.handle(dataBuffer, dataBufferOffset, isSupportCompress);
        switch (result.getCode()) {
            case REACH_END_BUFFER:
            case STLL_DATA_REMING:
                byte[] packetData = result.getPacketData();
                if (packetData != null) {
                    if (isEndOfDataFile(packetData)) {
                        loadDataHandler.end(packetData[3]);
                    } else {
                        loadDataHandler.handle(packetData);
                    }
                    return new ProtoHandlerResult(result.getCode(), result.getOffset());
                }
                return result;
            default:
                return result;
        }
    }


    private boolean isEndOfDataFile(byte[] data) {
        return (data.length == 4 && data[0] == 0 && data[1] == 0 && data[2] == 0);
    }
}
