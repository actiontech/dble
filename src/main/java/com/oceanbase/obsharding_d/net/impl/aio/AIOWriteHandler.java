/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.impl.aio;

import com.oceanbase.obsharding_d.net.service.CloseType;
import com.oceanbase.obsharding_d.net.service.ServiceTaskFactory;
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
                wr.con.pushServiceTask(ServiceTaskFactory.getInstance(wr.con.getService()).createForForceClose("write errno " + result, CloseType.WRITE));
            }
        } catch (Exception e) {
            wr.setWriteDataErr(true);
            AIOSocketWR.LOGGER.info("caught aio process err:", e);
        }

    }

    @Override
    public void failed(Throwable exc, AIOSocketWR wr) {
        wr.writing.set(false);
        wr.setWriteDataErr(true);
        if (Objects.equals(exc.getMessage(), "Broken pipe") || Objects.equals(exc.getMessage(), "Connection reset by peer") || exc instanceof ClosedChannelException) {
            // target problem,
            //ignore this exception,will close by read side.
            LOGGER.warn("Connection was closed while write. Detail reason:{}. {}.", exc, wr.con.getService());
        } else {
            //self problem.
            LOGGER.info("con {} write err:{}", wr.con.getService(), exc.getMessage());
            wr.con.pushServiceTask(ServiceTaskFactory.getInstance(wr.con.getService()).createForForceClose(exc.getMessage(), CloseType.WRITE));
        }
    }

}
