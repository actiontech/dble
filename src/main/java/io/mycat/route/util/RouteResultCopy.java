/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.util;

import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;

/*
 * @author guoji.ma@gmail.com
 *
 * maybe implement it in class RouteResultset and RouteResultsetNode using clone, but we onle need partly information.
 * So here.
 */
public final class RouteResultCopy {
    	public static RouteResultsetNode RRNCopy(RouteResultsetNode node, int sqlType, String stmt) {
	    	RouteResultsetNode nn = new RouteResultsetNode(node.getName(), sqlType, stmt);
		nn.setRunOnSlave(node.getRunOnSlave());
		nn.setCanRunInReadDB(node.getCanRunInReadDB());
		nn.setLimitSize(0);

		return nn;
	}

    	public static RouteResultset RRCopy(RouteResultset rrs, int sqlType, String stmt) {
	    	RouteResultset rr = new RouteResultset(stmt, sqlType, rrs.getSession());
		rr.setRunOnSlave(rrs.getRunOnSlave());
		rr.setFinishedRoute(rrs.isFinishedRoute());
		rr.setGlobalTable(rrs.isGlobalTable());
		rr.setAutocommit(rrs.isAutocommit());
		rr.setCanRunInReadDB(rrs.getCanRunInReadDB());
		
		RouteResultsetNode[] ns = rrs.getNodes();
		RouteResultsetNode[] nodes = new RouteResultsetNode[ns.length];
		for (int i = 0; i < ns.length; i++) {
		    	nodes[i] = RRNCopy(ns[i], sqlType, stmt);
			nodes[i].setSource(rr);
		}
		rr.setNodes(nodes);

		return rr;
	}
}
