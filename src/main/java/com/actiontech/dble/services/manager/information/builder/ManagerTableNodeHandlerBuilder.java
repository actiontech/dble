/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.builder;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.manager.ManagerBaseSelectHandler;
import com.actiontech.dble.plan.node.ManagerTableNode;
import com.actiontech.dble.services.manager.ManagerSession;

import java.util.ArrayList;
import java.util.List;

class ManagerTableNodeHandlerBuilder extends ManagerBaseHandlerBuilder {
    private ManagerTableNode node;

    ManagerTableNodeHandlerBuilder(ManagerSession session, ManagerTableNode node, ManagerHandlerBuilder hBuilder) {
        super(session, node, hBuilder);
        this.node = node;
        this.needWhereHandler = node.getWhereFilter() != null;
    }

    @Override
    protected void handleSubQueries() {
        handleBlockingSubQuery();
    }

    @Override
    public List<DMLResponseHandler> buildPre() {
        return new ArrayList<>();
    }

    @Override
    public void buildOwn() {
        ManagerBaseSelectHandler baseSelectHandler = new ManagerBaseSelectHandler(getSequenceId(), session, node);
        addHandler(baseSelectHandler);

        this.needSendMaker = baseSelectHandler.isNeedSendMaker();
    }

}
