/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.singleton;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.handler.HintHandler;
import com.actiontech.dble.route.handler.HintHandlerFactory;
import com.actiontech.dble.route.handler.HintSQLHandler;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
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
                                int sqlType, String stmt, ServerConnection sc) throws SQLException {
        return this.route(schema, sqlType, stmt, sc, false);
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ServerConnection sc, boolean isExplain)
            throws SQLException {
        RouteResultset rrs;
        String cacheKey = null;

        /*
         *  SELECT  SQL,  not cached in debug mode
         */
        if (sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null) {
            cacheKey = (schema == null ? "NULL" : schema.getName()) + "_" + sc.getUser() + "_" + stmt;
            rrs = (RouteResultset) CacheService.getSqlRouteCache().get(cacheKey);
            if (rrs != null) {
                sc.getSession2().endParse();
                return rrs;
            }
        }

        /*!dble: sql = select name from aa */
        /*!dble: sharding = test */
        int hintLength = RouteService.isHintSql(stmt);
        if (hintLength != -1) {
            int endPos = stmt.indexOf("*/");
            if (endPos > 0) {
                // router by hint of !dble:
                String hint = stmt.substring(hintLength, endPos).trim();

                String hintSplit = "=";
                int firstSplitPos = hint.indexOf(hintSplit);
                if (firstSplitPos > 0) {
                    Map hintMap = parseKeyValue(hint);
                    String hintType = (String) hintMap.get(HINT_TYPE);
                    String hintSql = (String) hintMap.get(hintType);
                    if (hintSql.length() == 0) {
                        String msg = "comment in sql must meet :/*!" + Versions.ANNOTATION_NAME + "type=value*/ or /*#" + Versions.ANNOTATION_NAME + "type=value*/ or /*" + Versions.ANNOTATION_NAME + "type=value*/: " + stmt;
                        LOGGER.info(msg);
                        throw new SQLSyntaxErrorException(msg);
                    }
                    String realSQL = stmt.substring(endPos + "*/".length()).trim();

                    HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
                    if (hintHandler != null) {
                        if (hintHandler instanceof HintSQLHandler) {
                            int hintSqlType = ServerParse.parse(hintSql) & 0xff;
                            rrs = hintHandler.route(schema, sqlType, realSQL, sc, hintSql, hintSqlType, hintMap);
                            // HintSQLHandler will always send to master
                            rrs.setRunOnSlave(false);
                        } else {
                            rrs = hintHandler.route(schema, sqlType, realSQL, sc, hintSql, sqlType, hintMap);
                        }
                    } else {
                        String msg = "Not supported hint sql type : " + hintType;
                        LOGGER.info(msg);
                        throw new SQLSyntaxErrorException(msg);
                    }
                } else { //fixed by runfriends@126.com
                    String msg = "comment in sql must meet :/*!" + Versions.ANNOTATION_NAME + "type=value*/ or /*#" + Versions.ANNOTATION_NAME + "type=value*/ or /*" + Versions.ANNOTATION_NAME + "type=value*/: " + stmt;
                    LOGGER.info(msg);
                    throw new SQLSyntaxErrorException(msg);
                }
            } else {
                stmt = stmt.trim();
                rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt, sc, isExplain);
            }
        } else {
            stmt = stmt.trim();
            rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt, sc, isExplain);
        }

        if (rrs != null && sqlType == ServerParse.SELECT && rrs.isSqlRouteCacheAble() && !LOGGER.isDebugEnabled() && CacheService.getSqlRouteCache() != null &&
                sc.getSession2().getRemingSql() == null) {
            CacheService.getSqlRouteCache().putIfAbsent(cacheKey, rrs);
        }
        return rrs;
    }

    private static int isHintSql(String sql) {
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

    private Map parseKeyValue(String substring) {
        Map<String, String> map = new HashMap<>();
        int indexOf = substring.indexOf('=');
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

}
