/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by zagnix on 2016/6/3.
 */
public class SerializerManager {

    /**
     * Wrap an output stream for compression if block compression is enabled for its block type
     */
    public OutputStream wrapForCompression(ConnectionId blockId, OutputStream s) {
        return s;
    }

    /**
     * Wrap an input stream for compression if block compression is enabled for its block type
     */
    public InputStream wrapForCompression(ConnectionId blockId, InputStream s) {
        return s;
    }

}
