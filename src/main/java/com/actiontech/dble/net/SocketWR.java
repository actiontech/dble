/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

import java.io.IOException;
import java.nio.ByteBuffer;


public abstract class SocketWR {
    public abstract void asyncRead() throws IOException;

    public abstract void doNextWriteCheck();

    public abstract boolean registerWrite(ByteBuffer buffer);

    public abstract void disableRead();

    public abstract void enableRead();

    public abstract void initFromConnection(com.actiontech.dble.net.connection.AbstractConnection connection);
}
