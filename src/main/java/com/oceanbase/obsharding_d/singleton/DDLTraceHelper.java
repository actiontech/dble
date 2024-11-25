/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class DDLTraceHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger("DDL_TRACE");
    private static final long DDL_EXECUTE_LIMIT = 30 * 60 * 60;

    // prefix
    private static final String NOTIFIED_PREFIX = "[DDL_NOTIFIED]";

    private static final DDLTraceHelper INSTANCE = new DDLTraceHelper();
    private final AtomicInteger index;
    private final Map<ShardingService, DDLTraceInfo> traceMap;

    private DDLTraceHelper() {
        index = new AtomicInteger();
        traceMap = new ConcurrentHashMap<>();
    }

    public static void init(ShardingService s, String sql) {
        DDLTraceInfo traceInfo = new DDLTraceInfo(s, sql);
        INSTANCE.getTraceMap().put(s, traceInfo);
    }

    public static void log(ShardingService s, Consumer<DDLTraceInfo> consumer) {
        try {
            if (s != null) {
                Optional.ofNullable(INSTANCE.getTraceMap().get(s)).ifPresent(consumer);
            }
        } catch (Exception e) {
            LOGGER.warn("DDL traceLog an error occurs: {}", e.getMessage());
        }
    }

    public static void log2(ShardingService s, Stage stage0, String... context0) {
        if (s != null) {
            log(s, d -> d.info(stage0, context0));
        } else {
            String context = getContext(context0);
            LOGGER.info(NOTIFIED_PREFIX + " <{}> {}", stage0, context);
        }
    }

    public static void log2(ShardingService s, Stage stage0, Status status0, String... context0) {
        if (s != null) {
            log(s, d -> d.info(stage0, status0, context0));
        } else {
            String context = getContext(context0);
            if (status0 == Status.fail) {
                LOGGER.warn(NOTIFIED_PREFIX + " <{}.{}> {}", stage0, status0, context);
            } else {
                LOGGER.info(NOTIFIED_PREFIX + " <{}.{}> {}", stage0, status0, context);
            }

        }
    }

    public static void finish(ShardingService s) {
        log(s, d -> d.end());
        INSTANCE.getTraceMap().remove(s);
    }

    public static void printDDLOutOfLimit() {
        for (Map.Entry<ShardingService, DDLTraceInfo> entry : INSTANCE.getTraceMap().entrySet()) {
            if ((TimeUtil.currentTimeMillis() - entry.getValue().getStartTime()) > DDL_EXECUTE_LIMIT) {
                LOGGER.warn("[DDL_{}] <scheduler> this ddl{{}} execute for too long.", entry.getValue().getId(), entry.getValue().getSql());
            }
        }
    }

    public AtomicInteger getIndex() {
        return index;
    }

    public Map<ShardingService, DDLTraceInfo> getTraceMap() {
        return traceMap;
    }

    private static String getContext(String... params) { // Just take the first parameter
        if (params.length > 0) {
            return params[0];
        }
        return "";
    }

    public static class DDLTraceInfo {
        private final int id;
        private final String sql;
        private final long startTime;

        private volatile String fail = null;

        public DDLTraceInfo(ShardingService s, String sql) {
            this.id = INSTANCE.getIndex().incrementAndGet();
            this.sql = sql;
            this.startTime = System.currentTimeMillis();
            LOGGER.info("================ {} [DDL_{}] ================", Stage.init_ddl_trace, this.id);
            LOGGER.info("[DDL_{}] <{}> Routes end and Start ddl{{}} execution stage. In {}", this.id, Stage.init_ddl_trace, this.sql, s.getConnection());
        }

        public void info(Stage stage0, String... context0) {
            info0(null, stage0, null, context0);
        }

        public void info(Stage stage0, Status status0, String... context0) {
            info0(null, stage0, status0, context0);
        }

        public void infoByNode(String node0, Stage stage0, Status status0, String... context0) {
            info0(node0, stage0, status0, context0);
        }

        public void end() {
            if (fail == null) {
                LOGGER.info("[DDL_{}] <{}> Execute success", this.id, Stage.finish_ddl_trace);
            } else {
                LOGGER.warn("[DDL_{}] <{}> Execute fail, cause: {}", this.id, Stage.finish_ddl_trace, fail);
            }
            LOGGER.info("================ {} [DDL_{}] ================", Stage.finish_ddl_trace, this.id);
        }

        private void info0(String node0, Stage stage0, Status status0, String... context0) {
            String prefix;
            if (node0 == null) {
                prefix = "[DDL_" + this.id + "]";
            } else {
                prefix = "[DDL_" + this.id + "." + node0 + "]";
            }
            String context = getContext(context0);
            if (status0 == null) {
                LOGGER.info("{} <{}> {}", prefix, stage0, context);
            } else {
                if (status0 == Status.fail) {
                    this.fail = context;
                    LOGGER.warn("{} <{}.{}> {}", prefix, stage0, status0, context);
                } else {
                    LOGGER.info("{} <{}.{}> {}", prefix, stage0, status0, context);
                }
            }
        }

        public int getId() {
            return id;
        }

        public String getSql() {
            return sql;
        }

        public long getStartTime() {
            return startTime;
        }
    }

    public enum Stage {
        // self
        init_ddl_trace,
        notice_cluster_ddl_prepare,
        add_table_lock,
        test_ddl_conn,
        exec_ddl_sql,
        update_table_metadata,
        notice_cluster_ddl_complete,
        release_table_lock,
        finish_ddl_trace,

        // cluster
        receive_ddl_prepare,
        receive_ddl_complete,
    }

    public enum Status {
        start, get_conn, succ, fail
    }
}
