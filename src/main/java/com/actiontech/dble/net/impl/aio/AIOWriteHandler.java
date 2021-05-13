/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.impl.aio;

import com.actiontech.dble.net.service.ServiceTaskFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.Objects;

class AIOWriteHandler implements CompletionHandler<Integer, AIOSocketWR> {
    private static final Logger LOGGER = LogManager.getLogger(AIOWriteHandler.class);
    @Override
    public void completed(final Integer result, final AIOSocketWR wr) {
        try {

            wr.writing.set(false);

            if (result >= 0) {
                wr.onWriteFinished(result);
            } else {
                wr.con.pushInnerServiceTask(ServiceTaskFactory.getInstance(wr.con.getService()).createForForceClose("write errno " + result));
            }
        } catch (Exception e) {
            AIOSocketWR.LOGGER.info("caught aio process err:", e);
        }

    }

    @Override
    public void failed(Throwable exc, AIOSocketWR wr) {
        wr.writing.set(false);

        if (Objects.equals(exc.getMessage(), "Broken pipe") || exc instanceof ClosedChannelException) {
            // target problem,
            //ignore this exception,will close by read side.
            LOGGER.debug("Connection was closed while read. Detail reason:{}. {}.", exc, wr.con.getService());
        } else {
            //self problem.
            LOGGER.info("con {} write err:{}", wr.con.getService(), exc.getMessage());
            wr.con.pushInnerServiceTask(ServiceTaskFactory.getInstance(wr.con.getService()).createForForceClose(exc.getMessage()));
        }
    }

}
