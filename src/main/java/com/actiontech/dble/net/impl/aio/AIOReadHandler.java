/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.impl.aio;


import com.actiontech.dble.net.service.CloseType;
import com.actiontech.dble.net.service.ServiceTaskFactory;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

class AIOReadHandler implements CompletionHandler<Integer, AIOSocketWR> {
    @Override
    public void completed(final Integer i, final AIOSocketWR wr) {
        if (i > 0) {
            try {
                wr.con.onReadData(i);
                wr.con.asyncRead();
            } catch (IOException e) {
                wr.con.close("handle err:" + e);
            }
        } else if (i == -1) {
            wr.con.pushServiceTask(ServiceTaskFactory.getInstance(wr.con.getService()).createForGracefulClose("client closed", CloseType.READ));
        }
    }

    @Override
    public void failed(Throwable exc, AIOSocketWR wr) {
        wr.con.close(exc.toString());

    }
}
