/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlsharding;

import com.oceanbase.obsharding_d.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandler;
import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResult;
import com.oceanbase.obsharding_d.net.handler.LoadDataInfileHandler;

import java.nio.ByteBuffer;


/**
 * Created by ylz on 2021/3/16.
 */
public class LoadDataProtoHandlerImpl implements ProtoHandler {

    private final LoadDataInfileHandler loadDataHandler;
    private final MySQLProtoHandlerImpl mySQLProtoHandler;

    public LoadDataProtoHandlerImpl(LoadDataInfileHandler loadDataHandler, MySQLProtoHandlerImpl mySQLProtoHandler) {
        this.loadDataHandler = loadDataHandler;
        this.mySQLProtoHandler = mySQLProtoHandler;
    }

    @Override
    public ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset, boolean isSupportCompress, boolean isContainSSLData) {
        ProtoHandlerResult.ProtoHandlerResultBuilder resultBuilder = mySQLProtoHandler.handlerResultBuilder(dataBuffer, dataBufferOffset, isSupportCompress);
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

    public MySQLProtoHandlerImpl getMySQLProtoHandler() {
        return mySQLProtoHandler;
    }

    public static boolean isEndOfDataFile(byte[] data) {
        // Load Data's empty package
        return (data.length == 4 && data[0] == 0 && data[1] == 0 && data[2] == 0);
    }
}
