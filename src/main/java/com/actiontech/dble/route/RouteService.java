/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route;

import com.actiontech.dble.cache.CachePool;
import com.actiontech.dble.cache.CacheService;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SchemaConfig;
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

public class RouteService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);
    public static final String HINT_TYPE = "_serverHintType";
    private final CachePool sqlRouteCache;
    private final LayerCachePool tableId2DataNodeCache;


    public RouteService(CacheService cachService) {
        sqlRouteCache = cachService.getCachePool("SQLRouteCache");
        tableId2DataNodeCache = (LayerCachePool) cachService.getCachePool("TableID2DataNodeCache");
    }

    public LayerCachePool getTableId2DataNodeCache() {
        return tableId2DataNodeCache;
    }

    public RouteResultset route(SchemaConfig schema,
                                int sqlType, String stmt, ServerConnection sc)
            throws SQLException {
        RouteResultset rrs = null;
        String cacheKey = null;

        /**
         *  SELECT  SQL,  not cached in debug mode
         */
        if (sqlType == ServerParse.SELECT && !LOGGER.isDebugEnabled() && sqlRouteCache != null) {
            cacheKey = (schema == null ? "NULL" : schema.getName()) + "_" + sc.getUser() + "_" + stmt;
            rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
            if (rrs != null) {
                return rrs;
            }
        }

        /*!dble: sql = select name from aa */
        /*!dble: schema = test */
        int hintLength = RouteService.isHintSql(stmt);
        if (hintLength != -1) {
            int endPos = stmt.indexOf("*/");
            if (endPos > 0) {
                // router by hint of !dble:
                String hint = stmt.substring(hintLength, endPos).trim();

                String hintSplit = "=";
                int firstSplitPos = hint.indexOf(hintSplit);
                if (firstSplitPos > 0) {
                    Map hintMap = parseHint(hint);
                    String hintType = (String) hintMap.get(HINT_TYPE);
                    String hintSql = (String) hintMap.get(hintType);
                    if (hintSql.length() == 0) {
                        String msg = "comment in sql must meet :/*!" + Versions.ANNOTATION_NAME + "type=value*/ or /*#" + Versions.ANNOTATION_NAME + "type=value*/ or /*" + Versions.ANNOTATION_NAME + "type=value*/: " + stmt;
                        LOGGER.warn(msg);
                        throw new SQLSyntaxErrorException(msg);
                    }
                    String realSQL = stmt.substring(endPos + "*/".length()).trim();

                    HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
                    if (hintHandler != null) {

                        if (hintHandler instanceof HintSQLHandler) {
                            int hintSqlType = ServerParse.parse(hintSql) & 0xff;
                            rrs = hintHandler.route(schema, sqlType, realSQL, sc, tableId2DataNodeCache, hintSql, hintSqlType, hintMap);

                        } else {
                            rrs = hintHandler.route(schema, sqlType, realSQL, sc, tableId2DataNodeCache, hintSql, sqlType, hintMap);
                        }

                    } else {
                        LOGGER.warn("TODO , support hint sql type : " + hintType);
                    }

                } else { //fixed by runfriends@126.com
                    String msg = "comment in sql must meet :/*!" + Versions.ANNOTATION_NAME + "type=value*/ or /*#" + Versions.ANNOTATION_NAME + "type=value*/ or /*" + Versions.ANNOTATION_NAME + "type=value*/: " + stmt;
                    LOGGER.warn(msg);
                    throw new SQLSyntaxErrorException(msg);
                }
            }
        } else {
            stmt = stmt.trim();
            rrs = RouteStrategyFactory.getRouteStrategy().route(schema, sqlType, stmt,
                    sc, tableId2DataNodeCache);
        }

        if (rrs != null && sqlType == ServerParse.SELECT && rrs.isCacheAble() && !LOGGER.isDebugEnabled() && sqlRouteCache != null) {
            sqlRouteCache.putIfAbsent(cacheKey, rrs);
        }
        return rrs;
    }

    public static int isHintSql(String sql) {
        char[] annotation = Versions.ANNOTATION_NAME.toCharArray();
        int j = 0;
        int len = sql.length();
        if (sql.charAt(j++) == '/' && sql.charAt(j++) == '*') {
            char c = sql.charAt(j);
            // support: "/** !dble: */" for mybatis and "/** #dble: */"  for mysql
            while (j < len && c != '!' && c != '#' && (c == ' ' || c == '*')) {
                c = sql.charAt(++j);
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

    private Map parseHint(String sql) {
        Map map = new HashMap();
        int y = 0;
        int begin = 0;
        for (int i = 0; i < sql.length(); i++) {
            char cur = sql.charAt(i);
            if (cur == ',' && y % 2 == 0) {
                String substring = sql.substring(begin, i);

                parseKeyValue(map, substring);
                begin = i + 1;
            } else if (cur == '\'') {
                y++;
            }
            if (i == sql.length() - 1) {
                parseKeyValue(map, sql.substring(begin));

            }


        }
        return map;
    }

    private void parseKeyValue(Map map, String substring) {
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
    }
}
