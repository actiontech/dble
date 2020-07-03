/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public interface NIOConnection extends ClosableConnection {

    /**
     * connected
     */
    void register() throws IOException;

    /**
     * execute
     */
    void handle(byte[] data);

    /**
     * writeDirectly from buffer
     */
    void write(ByteBuffer buffer);


}
