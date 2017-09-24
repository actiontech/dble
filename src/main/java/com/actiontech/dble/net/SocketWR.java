/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

import java.io.IOException;


public abstract class SocketWR {
    public abstract void asynRead() throws IOException;

    public abstract void doNextWriteCheck();
}
