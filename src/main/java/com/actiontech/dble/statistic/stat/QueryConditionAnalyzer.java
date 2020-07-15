/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.stat.TableStat.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * QueryConditionAnalyzer
 * --------------------------------------------------
 * <p>
 * ge:
 * SELECT * FROM v1user Where userName=? AND cityName =?
 * SELECT * FROM v1user Where userName=?
 * SELECT * FROM v1user Where userName=? AND age > 20
 * <p>
 * SELECT * FROM v1user Where userName = "A" AND cityName = "BEIJING";
 * SELECT * FROM v1user Where userName = "B"
 * SELECT * FROM v1user Where userName = "A" AND age > 20
 * <p>
 * If we want to konw the userName frequency
 * <p>
 * set: tablename&column  ( v1user&userName ), set NULL for cancel
 *
 * @author zhuam
 */
public final class QueryConditionAnalyzer implements QueryResultListener {
    private static final long MAX_QUERY_MAP_SIZE = 100000;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryConditionAnalyzer.class);

    private String tableName = null;
    private String columnName = null;

    private final Map<Object, AtomicLong> map = new ConcurrentHashMap<>();

    private ReentrantLock lock = new ReentrantLock();

    private SQLParser sqlParser = new SQLParser();

    private static final QueryConditionAnalyzer INSTANCE = new QueryConditionAnalyzer();

    private QueryConditionAnalyzer() {
    }

    public static QueryConditionAnalyzer getInstance() {
        return INSTANCE;
    }


    @Override
    public void onQueryResult(QueryResult queryResult) {
        int sqlType = queryResult.getSqlType();
        String sql = queryResult.getSql();
        if (sqlType == ServerParse.SELECT) {
            List<Object> values = sqlParser.parseConditionValues(sql, this.tableName, this.columnName);
            if (values != null) {
                if (this.map.size() < MAX_QUERY_MAP_SIZE) {
                    for (Object value : values) {
                        AtomicLong count = this.map.get(value);
                        if (count == null) {
                            count = new AtomicLong(1L);
                        } else {
                            count.getAndIncrement();
                        }
                        this.map.put(value, count);
                    }
                } else {
                    LOGGER.debug(" this map is too large size ");
                }
            }
        }
    }

    public boolean setCf(String cf) {

        boolean isOk = false;

        this.lock.lock();
        try {

            if (!"NULL".equalsIgnoreCase(cf)) {

                String[] tableColumn = cf.split("&");
                if (tableColumn != null && tableColumn.length == 2) {
                    this.tableName = tableColumn[0];
                    this.columnName = tableColumn[1];
                    this.map.clear();

                    isOk = true;
                }

            } else {

                this.tableName = null;
                this.columnName = null;
                this.map.clear();

                isOk = true;
            }

        } finally {
            this.lock.unlock();
        }

        return isOk;
    }

    public String getKey() {
        return this.tableName + "." + this.columnName;
    }

    public List<Map.Entry<Object, AtomicLong>> getValues() {
        List<Map.Entry<Object, AtomicLong>> list = new ArrayList<>(map.entrySet());
        return list;
    }


    class SQLParser {

        /**
         * fixName :sharding and `
         *
         * @param table
         * @return
         */
        private String fixName(String table) {
            if (table != null) {
                table = table.replace("`", "");
                int dotIdx = table.indexOf(".");
                if (dotIdx > 0) {
                    table = table.substring(1 + dotIdx).trim();
                }
            }
            return table;
        }

        /**
         * parseConditionValues
         *
         * @param sql
         * @param table
         * @param column
         * @return
         */
        public List<Object> parseConditionValues(String sql, String table, String column) {

            List<Object> values = null;

            if (sql != null && table != null && QueryConditionAnalyzer.this.columnName != null) {

                values = new ArrayList<>();

                MySqlStatementParser parser = new MySqlStatementParser(sql);
                SQLStatement stmt = parser.parseStatement();

                ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
                stmt.accept(visitor);

                String currentTable = visitor.getCurrentTable();
                if (table.equalsIgnoreCase(currentTable)) {

                    List<Condition> conditions = visitor.getConditions();
                    for (Condition condition : conditions) {

                        String ccN = condition.getColumn().getName();
                        ccN = fixName(ccN);

                        if (column.equalsIgnoreCase(ccN)) {
                            List<Object> ccVL = condition.getValues();
                            values.addAll(ccVL);
                        }
                    }
                }
            }
            return values;
        }
    }
}
