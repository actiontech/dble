/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.factorys;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.OutputHandlerForPrepare;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.variables.OutputStateEnum;

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
