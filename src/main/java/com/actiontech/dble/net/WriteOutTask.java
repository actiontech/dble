/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/7/7.
 */
public class WriteOutTask {

    private final ByteBuffer buffer;
    private final boolean closeFlag;

    public WriteOutTask(ByteBuffer buffer, boolean closeFlag) {
        this.buffer = buffer;
        this.closeFlag = closeFlag;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public boolean closeFlag() {
        return closeFlag;
    }
}
