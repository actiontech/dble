package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.handler.LoadDataInfileHandler;

import java.nio.ByteBuffer;


/**
 * Created by ylz on 2021/3/16.
 */
public class LoadDataProtoHandlerImpl extends MySQLProtoHandlerImpl {

    private final LoadDataInfileHandler loadDataHandler;

    public LoadDataProtoHandlerImpl(LoadDataInfileHandler loadDataHandler) {
        this.loadDataHandler = loadDataHandler;
    }

    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset, boolean isSupportCompress) {
        ProtoHandlerResult.ProtoHandlerResultBuilder resultBuilder = handlerResultBuilder(dataBuffer, dataBufferOffset, isSupportCompress);
        ProtoHandlerResult result = resultBuilder.build();
        switch (result.getCode()) {
            case COMPLETE_PACKET:
                byte[] packetData = result.getPacketData();
                if (packetData != null) {
                    if (isEndOfDataFile(packetData)) {
                        loadDataHandler.end(packetData[3]);
                    } else {
                        loadDataHandler.handle(packetData);
                    }
                    return resultBuilder.setPacketData(null).build();
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
