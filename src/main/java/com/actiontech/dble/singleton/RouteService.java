/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.handler.HintHandler;
import com.actiontech.dble.route.handler.HintHandlerFactory;
import com.actiontech.dble.route.handler.HintSQLHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

public final class RouteService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);
    private static final String HINT_TYPE = "_serverHintType";
    private static final RouteService INSTANCE = new RouteService();

    private RouteService() {
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ShardingService service) throws SQLException {
        return this.route(schema, sqlType, stmt, service, false);
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ShardingService service, boolean isExplain)
            throws SQLException {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "simple-route");
        RouteResultset rrs = null;
        try {
            String cacheKey = null;

            if (sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null) {
                cacheKey = (schema == null ? "NULL" : schema.getName()) + "_" + service.getUser() + "_" + stmt;
                rrs = (RouteResultset) CacheService.getSqlRouteCache().get(cacheKey);
                if (rrs != null) {
                    service.getSession2().endParse();
                    return rrs;
                }
            }

            int hintLength = RouteService.isHintSql(stmt);
            if (hintLength != -1) {
                int endPos = stmt.substring(hintLength).indexOf("*/") + hintLength;
                if (endPos > 0) {
                    rrs = routeHint(stmt, hintLength, endPos, schema, sqlType, service);
                } else {
                    stmt = stmt.trim();
                    rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt, service, isExplain);
                }
            } else {
                stmt = stmt.trim();
                rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt, service, isExplain);
            }

            if (rrs != null && sqlType == ServerParse.SELECT && rrs.isSqlRouteCacheAble() && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null &&
                    service.getSession2().getRemingSql() == null) {
                CacheService.getSqlRouteCache().putIfAbsent(cacheKey, rrs);
            }
            return rrs;
        } finally {
            if (rrs != null) {
                TraceManager.log(ImmutableMap.of("route-result-set", rrs), traceObject);
            }
            TraceManager.finishSpan(service, traceObject);
        }
    }

    public PhysicalDbInstance routeRwSplit(int sqlType, String stmt, RWSplitService service) throws SQLException {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "rw-split-hint-simple-route");
        PhysicalDbInstance dbInstance = null;
        try {
            String cacheKey = null;

            if (sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null) {
                RwSplitUserConfig rwSplitUserConfig = (RwSplitUserConfig) service.getUserConfig();
                String dbGroup = rwSplitUserConfig.getDbGroup();
                cacheKey = dbGroup + "_" + service.getUser() + "_" + stmt;
                dbInstance = (PhysicalDbInstance) CacheService.getSqlRouteCache().get(cacheKey);
                if (dbInstance != null) {
                    return dbInstance;
                }
            }

            int hintLength = RouteService.isHintSql(stmt);
            if (hintLength != -1) {
                int endPos = stmt.substring(hintLength).indexOf("*/") + hintLength;
                if (endPos > 0) {
                    dbInstance = routeRwSplitHint(stmt, hintLength, endPos, sqlType, service);
                }
            }

            if (dbInstance != null && sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null) {
                CacheService.getSqlRouteCache().putIfAbsent(cacheKey, dbInstance);
            }
            return dbInstance;
        } finally {
            if (dbInstance != null) {
                TraceManager.log(ImmutableMap.of("rw-split-hint-route-result-set", dbInstance), traceObject);
            }
            TraceManager.finishSpan(service, traceObject);
        }
    }

    private RouteResultset routeHint(String stmt, int hintLength, int endPos, SchemaConfig schema, int sqlType, ShardingService service) throws SQLException {
        RouteResultset rrs;
        HintInfo hintInfo = parseHintSql(stmt, hintLength, endPos);
        HintHandler hintHandler = HintHandlerFactory.getDbleHintHandler(hintInfo.getHintType());
        if (hintHandler == null) {
            String msg = "Not supported hint sql type : " + hintInfo.getHintType();
            LOGGER.info(msg);
            throw new SQLSyntaxErrorException(msg);
        }
        if (hintHandler instanceof HintSQLHandler) {
            int hintSqlType = ServerParse.parse(hintInfo.getHintSQL()) & 0xff;
            rrs = hintHandler.route(schema, sqlType, hintInfo.getRealSQL(), service, hintInfo.getHintSQL(), hintSqlType, hintInfo.getHintMap());
            // HintSQLHandler will always send to master
            rrs.setRunOnSlave(false);
        } else {
            rrs = hintHandler.route(schema, sqlType, hintInfo.getRealSQL(), service, hintInfo.getHintSQL(), sqlType, hintInfo.getHintMap());
        }
        return rrs;
    }

    private PhysicalDbInstance routeRwSplitHint(String stmt, int hintLength, int endPos, int sqlType, RWSplitService service) throws SQLException {
        HintInfo hintInfo = parseHintSql(stmt, hintLength, endPos);
        HintHandler hintHandler = HintHandlerFactory.getRwSplitHintHandler(hintInfo.getHintType());
        if (hintHandler == null) {
            String msg = "Not supported hint sql type : " + hintInfo.getHintType();
            LOGGER.info(msg);
            throw new SQLSyntaxErrorException(msg);
        }
        service.setExecuteSql(hintInfo.getRealSQL());
        return hintHandler.routeRwSplit(sqlType, hintInfo.getRealSQL(), service, hintInfo.getHintSQL(), sqlType, hintInfo.getHintMap());
    }

    private HintInfo parseHintSql(String stmt, int hintLength, int endPos) throws SQLSyntaxErrorException {
        String hint = stmt.substring(hintLength, endPos).trim();
        String realSQL = stmt.substring(endPos + "*/".length()).trim();
        String hintSql;
        Map hintMap = null;
        String hintType = null;
        if (hint.indexOf("=") >= 0) {
            //sql/sharddingNode/db_type/db_instance_url=*****
            hintMap = parseKeyValue(hint, '=');
            hintType = (String) hintMap.get(HINT_TYPE);
            hintSql = (String) hintMap.get(hintType);
        } else if (hint.indexOf(":") >= 0) {
            //uproxy_dest:
            String left = stmt.substring(0, hintLength - 2);
            String right = stmt.substring(endPos + 2);
            realSQL = left + right;
            hintMap = parseKeyValue(hint, ':');
            hintType = (String) hintMap.get(HINT_TYPE);
            hintSql = (String) hintMap.get(hintType);
        } else {
            //master
            String left = stmt.substring(0, hintLength - 2);
            String right = stmt.substring(endPos + 2);
            realSQL = left + right;
            hintType = hint;
            hintSql = hint;
        }
        if (hintSql.length() == 0) {
            String msg = "comment in sql must meet :/*!" + Versions.ANNOTATION_NAME + "type=value*/ or /*#" + Versions.ANNOTATION_NAME + "type=value*/ or /*" + Versions.ANNOTATION_NAME + "type=value*/: " + stmt;
            LOGGER.info(msg);
            throw new SQLSyntaxErrorException(msg);
        }
        return new HintInfo(hint, hintMap, hintType, hintSql, realSQL);
    }

    public static int isHintSql(String stmt) {
        int hintIndex = isDbleHintSql(stmt);
        if (hintIndex == -1) {
            hintIndex = isUproxyHintSql(stmt);
        }
        return hintIndex;
    }

    private static int isUproxyHintSql(String sql) {
        int index;
        if (StringUtil.isEmpty(sql) || (index = sql.indexOf("/*")) < 0) {
            return -1;
        }
        String[] leftSplit = sql.split("/\\*");
        if (leftSplit.length != 2 || !leftSplit[1].contains("*/")) {
            return -1;
        }
        String[] rightSplit = leftSplit[1].split("\\*/");
        String content = rightSplit[0].trim();
        if (StringUtil.equalsIgnoreCase(content, "master")) {
            return index + 2;
        } else if (content.length() > 12 && content.substring(0, 12).equalsIgnoreCase("uproxy_dest:")) {
            return index + 2;
        }
        return -1;
    }

    private static int isDbleHintSql(String sql) {
        char[] annotation = Versions.ANNOTATION_NAME.toCharArray();
        int j = 0;
        int len = sql.length();
        if (sql.charAt(j++) == '/' && sql.charAt(j++) == '*') {
            char c = sql.charAt(j);
            // support: "/*#dble: */" for mybatis and "/*!dble: */"  for mysql
            while (c == ' ') {
                c = sql.charAt(++j);
            }
            if (c != '!' && c != '#') {
                return -1;
            }
            if (sql.charAt(j) == annotation[0]) {
                j--;
            }
            if (j + 6 >= len) { // prevent the following sql.charAt overflow
                return -1;        // false
            }

            for (int i = 0; i < annotation.length; i++) {
                if (sql.charAt(++j) != annotation[i]) {
                    break;
                }
                if (i == annotation.length - 1) {
                    return j + 1;
                }
            }
        }
        return -1;    // false
    }

    private Map parseKeyValue(String substring, char splitChar) {
        Map<String, String> map = new HashMap<>();
        int indexOf = substring.indexOf(splitChar);
        if (indexOf != -1) {

            String key = substring.substring(0, indexOf).trim().toLowerCase();
            String value = substring.substring(indexOf + 1, substring.length());
            if (value.endsWith("'") && value.startsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            if (map.isEmpty()) {
                map.put(HINT_TYPE, key);
            }
            map.put(key, value.trim());

        }
        return map;
    }

    public static RouteService getInstance() {
        return INSTANCE;
    }

    static class HintInfo {
        private String hint;
        private Map hintMap;
        private String hintType;
        private String hintSQL;
        private String realSQL;

        HintInfo(String hint, Map hintMap, String hintType, String hintSQL, String realSQL) {
            this.hint = hint;
            this.hintMap = hintMap;
            this.hintType = hintType;
            this.hintSQL = hintSQL;
            this.realSQL = realSQL;
        }

        public String getHint() {
            return hint;
        }

        public void setHint(String hint) {
            this.hint = hint;
        }

        public Map getHintMap() {
            return hintMap;
        }

        public void setHintMap(Map hintMap) {
            this.hintMap = hintMap;
        }

        public String getHintType() {
            return hintType;
        }

        public void setHintType(String hintType) {
            this.hintType = hintType;
        }

        public String getHintSQL() {
            return hintSQL;
        }

        public void setHintSQL(String hintSQL) {
            this.hintSQL = hintSQL;
        }

        public String getRealSQL() {
            return realSQL;
        }

        public void setRealSQL(String realSQL) {
            this.realSQL = realSQL;
        }
    }

}
