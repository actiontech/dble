/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.proto.handler;

import com.oceanbase.obsharding_d.util.exception.NotSslRecordException;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/6/16.
 */
public interface ProtoHandler {

    ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset, boolean isSupportCompress, boolean isContainSSLData) throws NotSslRecordException;

}
