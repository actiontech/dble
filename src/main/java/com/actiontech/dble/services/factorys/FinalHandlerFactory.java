/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.factorys;

import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandlerForPrepare;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.variables.OutputStateEnum;

/**
 * Created by szf on 2020/6/28.
 */
public final class FinalHandlerFactory {

    private FinalHandlerFactory() {
    }

    public static OutputHandler createFinalHandler(NonBlockingSession session) {
        final OutputStateEnum outputState = session.getShardingService().getRequestScope().getOutputState();

        switch (outputState) {
            case NORMAL_QUERY:
                return new OutputHandler(BaseHandlerBuilder.getSequenceId(), session);
            case PREPARE:
                return new OutputHandlerForPrepare(BaseHandlerBuilder.getSequenceId(), session);
            default:
                throw new UnsupportedOperationException("illegal outputState");
        }


    }


}
