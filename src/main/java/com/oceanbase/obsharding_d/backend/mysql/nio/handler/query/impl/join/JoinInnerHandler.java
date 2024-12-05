/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.join;

import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.util.List;

/**
 * Created by szf on 2019/5/31.
 */
public class JoinInnerHandler extends JoinHandler {

    public JoinInnerHandler(long id, NonBlockingSession session, boolean isLeftJoin, List<Order> leftOrder, List<Order> rightOrder, Item otherJoinOn) {
        super(id, session, isLeftJoin, leftOrder, rightOrder, otherJoinOn);
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.INNER_FUNC_ADD;
    }
}
