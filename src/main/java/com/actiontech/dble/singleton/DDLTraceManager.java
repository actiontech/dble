package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DDLTraceInfo;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2019/11/29.
 */
public final class DDLTraceManager {

    private static final long DDL_EXECUTE_LIMIT = 30 * 60 * 60;
    private static final String DDL_TRACE_LOG = "DDL_TRACE";
    private static final Logger LOGGER = LoggerFactory.getLogger(DDL_TRACE_LOG);
    private static final DDLTraceManager INSTANCE = new DDLTraceManager();
    private final AtomicInteger index = new AtomicInteger();
    private final Map<ServerConnection, DDLTraceInfo> traceMap = new ConcurrentHashMap<>();

    private DDLTraceManager() {
    }


    public void startDDL(ServerConnection c) {
        DDLTraceInfo info = new DDLTraceInfo(c.getId(), c.getExecuteSql(), index.getAndIncrement());
        traceMap.put(c, info);
        LOGGER.info("NEW DDL START:" + info.toString());
    }

    public void updateConnectionStatus(ServerConnection sc, MySQLConnection mc, DDLTraceInfo.DDLConnectionStatus status) {
        DDLTraceInfo info = traceMap.get(sc);
        if (info != null) {
            info.updateConnectionStatus(mc, status);
            LOGGER.info("[DDL][{}] MySQLConnection status update : backendId = {} mysqlId = {} {}", info.getId(), mc.getId(), mc.getThreadId(), status);
        }
    }

    public void updateRouteNodeStatus(ServerConnection sc, RouteResultsetNode routeNode, DDLTraceInfo.DDLConnectionStatus status) {
        DDLTraceInfo info = traceMap.get(sc);
        if (info != null) {
            LOGGER.info("[DDL][{}] cant connect to {} status {}", info.getId(), routeNode.getName(), status);
        }
    }

    public void updateDDLStatus(DDLTraceInfo.DDLStage stage, ServerConnection c) {
        DDLTraceInfo info = traceMap.get(c);
        if (info != null) {
            info.setStage(stage);
            LOGGER.info("STAGE CHANGE " + info.toBriefString());
        }
    }


    public void endDDL(ServerConnection c, String reason) {
        DDLTraceInfo info = traceMap.get(c);
        if (info != null) {
            info.setStage(DDLTraceInfo.DDLStage.EXECUTE_END);
            LOGGER.info((reason == null ? "DDL END:" : "DDL END WITH REASON:" + reason) + info.toString());
            traceMap.remove(c);
        }
    }

    public static DDLTraceManager getInstance() {
        return INSTANCE;
    }

    public void printDDLOutOfLimit() {
        for (Map.Entry<ServerConnection, DDLTraceInfo> entry : traceMap.entrySet()) {
            if ((TimeUtil.currentTimeMillis() - entry.getValue().getStartTimestamp()) > DDL_EXECUTE_LIMIT) {
                LOGGER.warn("THIS DDL EXECUTE FOR TOO LONG \n" + entry.getValue().toString());
            }
        }
    }


}
