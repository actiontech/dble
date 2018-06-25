/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PauseStart {
    private static final Pattern PATTERN_FOR_PAUSE = Pattern.compile("^\\s*pause\\s*@@dataNode\\s*=\\s*'([a-zA-Z_0-9,]+)'\\s*and\\s*timeout\\s*=\\s*([0-9]+)\\s*$", 2);
    private static final OkPacket OK = new OkPacket();
    private static final Logger LOGGER = LoggerFactory.getLogger(PauseStart.class);

    private PauseStart() {
    }

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    public static void execute(ManagerConnection c, String sql) {

        boolean recycleFinish = false;

        Matcher ma = PATTERN_FOR_PAUSE.matcher(sql);
        if (!ma.matches()) {
            c.writeErrMessage(1105, "The sql did not match pause @@dataNode 'dn......' and timeout = ([0-9]+)");
            return;
        }
        String dataNode = ma.group(1);
        Set<String> dataNodes = new HashSet<>(Arrays.asList(dataNode.split(",")));
        //check dataNodes
        for (String singleDn : dataNodes) {
            if (DbleServer.getInstance().getConfig().getDataNodes().get(singleDn) == null) {
                c.writeErrMessage(1105, "DataNode " + singleDn + " did not exists");
                return;
            }
        }


        //clusterPauseNotic
        if (!DbleServer.getInstance().getMiManager().clusterPauseNotic(dataNode)) {
            c.writeErrMessage(1105, "Other node in cluster is pausing");
            return;
        }


        if (!DbleServer.getInstance().getMiManager().getIsPausing().compareAndSet(false, true)) {
            c.writeErrMessage(1003, "Some dataNodes is paused, please resume first");
            return;
        }


        DbleServer.getInstance().getMiManager().lockWithDataNodes(dataNodes);


        //self pause the dataNode
        long timeOut = Long.parseLong(ma.group(2)) * 1000;
        long beginTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - beginTime < timeOut) {
            boolean nextTurn = false;
            for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                    if ((entry.getValue() instanceof ServerConnection)) {
                        ServerConnection sconnection = (ServerConnection) entry.getValue();
                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : sconnection.getSession2().getTargetMap().entrySet()) {
                            if (dataNodes.contains(conEntry.getKey().getName())) {
                                nextTurn = true;
                                break;
                            }
                        }
                        if (nextTurn) {
                            break;
                        }
                    }
                }
                if (nextTurn) {
                    break;
                }
            }
            if (!nextTurn) {
                recycleFinish = true;
                break;
            }
            if (!recycleFinish) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }
        }

        LOGGER.info("wait finished " + recycleFinish);
        if (!recycleFinish) {
            DbleServer.getInstance().getMiManager().resume();
            try {
                DbleServer.getInstance().getMiManager().resumeCluster();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
            }
            c.writeErrMessage(1003, "The backend connection recycle failure,try it later");
        } else {
            try {
                if (DbleServer.getInstance().getMiManager().waitForCluster(c, beginTime, timeOut)) {
                    OK.write(c);
                }
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }
}
