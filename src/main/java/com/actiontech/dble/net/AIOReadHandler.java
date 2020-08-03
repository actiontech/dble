/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

class AIOReadHandler implements CompletionHandler<Integer, AIOSocketWR> {
    @Override
    public void completed(final Integer i, final AIOSocketWR wr) {
        // con.getProcessor().getExecutor().execute(new Runnable() {
        // public void run() {
        if (i > 0) {
            try {
                wr.con.onReadData(i);
                wr.con.asyncRead();
            } catch (IOException e) {
                wr.con.close("handle err:" + e);
            }
        } else if (i == -1) {
            if (wr.con instanceof MySQLConnection) {
                ((MySQLConnection) wr.con).closeInner("stream closed");
            } else {
                wr.con.close("client closed");
            }

        }
        // }
        // });
    }

    @Override
    public void failed(Throwable exc, AIOSocketWR wr) {
        wr.con.close(exc.toString());

    }
}
