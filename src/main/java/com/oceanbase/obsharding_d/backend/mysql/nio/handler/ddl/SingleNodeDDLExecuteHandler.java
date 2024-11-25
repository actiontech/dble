/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl;

import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import org.jetbrains.annotations.Nullable;

public class SingleNodeDDLExecuteHandler extends BaseDDLHandler {

    public SingleNodeDDLExecuteHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment, ImplicitlyCommitCallback implicitlyCommitCallback) {
        super(session, rrs, attachment, implicitlyCommitCallback);
    }
}
