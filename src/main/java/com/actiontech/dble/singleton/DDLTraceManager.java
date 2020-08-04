package com.actiontech.dble.singleton;


import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.cluster.values.DDLTraceInfo;

import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
    private final Map<ShardingService, DDLTraceInfo> traceMap = new ConcurrentHashMap<>();

    private DDLTraceManager() {
    }


    public void startDDL(ShardingService service) {
        DDLTraceInfo info = new DDLTraceInfo(service.getConnection().getId(), service.getExecuteSql(), index.getAndIncrement());
        traceMap.put(service, info);
        LOGGER.info("NEW DDL START:" + info.toString());
    }

    public void updateConnectionStatus(ShardingService service, MySQLResponseService mr, DDLTraceInfo.DDLConnectionStatus status) {
        DDLTraceInfo info = traceMap.get(service);
        if (info != null) {
            info.updateConnectionStatus(mr, status);
            LOGGER.info("[DDL][{}] MySQLConnection status update : backendId = {} mysqlId = {} {}", info.getId(), mr.getConnection().getId(), mr.getConnection().getThreadId(), status);
        }
    }

    public void updateRouteNodeStatus(ShardingService service, RouteResultsetNode routeNode, DDLTraceInfo.DDLConnectionStatus status) {
        DDLTraceInfo info = traceMap.get(service);
        if (info != null) {
            LOGGER.info("[DDL][{}] cant connect to {} status {}", info.getId(), routeNode.getName(), status);
        }
    }

    public void updateDDLStatus(DDLTraceInfo.DDLStage stage, ShardingService service) {
        DDLTraceInfo info = traceMap.get(service);
        if (info != null) {
            info.setStage(stage);
            LOGGER.info("STAGE CHANGE " + info.toBriefString());
        }
    }


    public void endDDL(ShardingService service, String reason) {
        DDLTraceInfo info = traceMap.get(service);
        if (info != null) {
            info.setStage(DDLTraceInfo.DDLStage.EXECUTE_END);
            LOGGER.info((reason == null ? "DDL END:" : "DDL END WITH REASON:" + reason) + info.toString());
            traceMap.remove(service);
        }
    }

    public static DDLTraceManager getInstance() {
        return INSTANCE;
    }

    public void printDDLOutOfLimit() {
        for (Map.Entry<ShardingService, DDLTraceInfo> entry : traceMap.entrySet()) {
            if ((TimeUtil.currentTimeMillis() - entry.getValue().getStartTimestamp()) > DDL_EXECUTE_LIMIT) {
                LOGGER.warn("THIS DDL EXECUTE FOR TOO LONG \n" + entry.getValue().toString());
            }
        }
    }


}
