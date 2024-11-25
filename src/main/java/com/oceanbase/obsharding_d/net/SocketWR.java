/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net;

import java.io.IOException;
import java.nio.ByteBuffer;


public abstract class SocketWR {
    public abstract boolean isWriteComplete();

    public abstract void asyncRead() throws IOException;

    public abstract void doNextWriteCheck();

    /**
     * return false will be filtered
     */
    public abstract boolean canNotWrite();

    public abstract boolean registerWrite(ByteBuffer buffer);

    public abstract void disableRead();

    public abstract void enableRead();

    public abstract void disableReadForever();

    public abstract void initFromConnection(com.oceanbase.obsharding_d.net.connection.AbstractConnection connection);

    /**
     * shutdownInput will trigger  a read event of IO reactor and return -1.
     *
     * @throws IOException
     */
    public abstract void shutdownInput() throws IOException;


    public abstract void closeSocket() throws IOException;
}
