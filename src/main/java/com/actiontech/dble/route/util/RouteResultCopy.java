/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.util;

import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;

/*
 * @author guoji.ma@gmail.com
 *
 * maybe implement it in class RouteResultset and RouteResultsetNode using clone, but we onle need partly information.
 * So here.
 */
public final class RouteResultCopy {
    private RouteResultCopy() {
    }

    public static RouteResultsetNode rrnCopy(RouteResultsetNode node, int sqlType, String stmt) {
        RouteResultsetNode nn = new RouteResultsetNode(node.getName(), sqlType, stmt);
        nn.setRunOnSlave(node.getRunOnSlave());
        nn.setCanRunInReadDB(true);
        nn.setLimitSize(0);
        return nn;
    }

    public static RouteResultset rrCopy(RouteResultset rrs, int sqlType, String stmt) {
        RouteResultset rr = new RouteResultset(stmt, sqlType);
        rr.setRunOnSlave(rrs.getRunOnSlave());
        rr.setFinishedRoute(rrs.isFinishedRoute());
        rr.setGlobalTable(rrs.isGlobalTable());
        rr.setCanRunInReadDB(rrs.getCanRunInReadDB());

        RouteResultsetNode[] ns = rrs.getNodes();
        RouteResultsetNode[] nodes = new RouteResultsetNode[ns.length];
        for (int i = 0; i < ns.length; i++) {
            nodes[i] = rrnCopy(ns[i], sqlType, stmt);
        }
        rr.setNodes(nodes);

        return rr;
    }
}
