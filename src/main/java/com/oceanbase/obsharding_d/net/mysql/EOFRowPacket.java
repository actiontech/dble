/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.mysql;

import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/7/8.
 */
public class EOFRowPacket extends EOFPacket {


    public void write(ByteBuffer buffer, AbstractService service) {
        service.writeWithBuffer(this, buffer);
    }

    @Override
    public boolean isEndOfQuery() {
        return true;
    }

    @Override
    public ResultFlag getResultFlag() {
        return ResultFlag.EOF_ROW;
    }
}
