/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.impl.aio;

import java.nio.channels.CompletionHandler;

class AIOWriteHandler implements CompletionHandler<Integer, AIOSocketWR> {

    @Override
    public void completed(final Integer result, final AIOSocketWR wr) {
        try {

            wr.writing.set(false);

            if (result >= 0) {
                wr.onWriteFinished(result);
            } else {
                wr.con.close("write erro " + result);
            }
        } catch (Exception e) {
            AIOSocketWR.LOGGER.info("caught aio process err:", e);
        }

    }

    @Override
    public void failed(Throwable exc, AIOSocketWR wr) {
        wr.writing.set(false);
        wr.con.close("write failed " + exc);
    }

}
