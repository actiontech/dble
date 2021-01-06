/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit.handle;

import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.services.rwsplit.RWSplitService;

import java.util.Set;

/**
 * @author dcy
 * Create Date: 2020-12-15
 */
public final class TempTableHandler {
    private TempTableHandler() {
    }

    public static void handleCreate(String stmt, RWSplitService service, int offset) {
        final int rightTerminatorOffset = stmt.indexOf("(");
        final String tableName = stmt.substring(offset, rightTerminatorOffset).trim();
        final RWSplitNonBlockingSession session = service.getSession();

        session.execute(true, (isSuccess, rwSplitService) -> {
            final Set<String> tempTableSet = rwSplitService.getTmpTableSet();
            tempTableSet.add(tableName);
            rwSplitService.setUsingTmpTable(true);
        });
    }


    public static void handleDrop(String stmt, RWSplitService service, int offset) {

        final String tableName = stmt.substring(offset).trim();
        final RWSplitNonBlockingSession session = service.getSession();

        session.execute(true, (isSuccess, rwSplitService) -> {
            final Set<String> tempTableSet = rwSplitService.getTmpTableSet();
            tempTableSet.remove(tableName);
            if (tempTableSet.isEmpty()) {
                rwSplitService.setUsingTmpTable(false);
            }

        });
    }
}
