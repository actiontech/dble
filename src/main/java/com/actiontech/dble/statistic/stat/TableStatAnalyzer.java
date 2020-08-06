/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.druid.util.JdbcConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TableStatAnalyzer
 *
 * @author zhuam
 */
public final class TableStatAnalyzer implements QueryResultListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableStatAnalyzer.class);

    private Map<String, TableStat> tableStatMap = new ConcurrentHashMap<>();
    private ReentrantLock lock = new ReentrantLock();

    //PARSER SQL TO GET NAME
    private SQLParser sqlParser = new SQLParser();

    private static final TableStatAnalyzer INSTANCE = new TableStatAnalyzer();

    private TableStatAnalyzer() {
    }

    public static TableStatAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void onQueryResult(QueryResult queryResult) {

        int sqlType = queryResult.getSqlType();
        String sql = queryResult.getSql();
        switch (sqlType) {
            case ServerParse.SELECT:
            case ServerParse.UPDATE:
            case ServerParse.INSERT:
            case ServerParse.DELETE:
            case ServerParse.REPLACE:

                String masterTable = null;
                List<String> relationTables = new ArrayList<>();

                List<String> tables = sqlParser.parseTableNames(sql);
                for (int i = 0; i < tables.size(); i++) {
                    String table = tables.get(i);
                    if (i == 0) {
                        masterTable = table;
                    } else {
                        relationTables.add(table);
                    }
                }

                if (masterTable != null) {
                    TableStat tableStat = getTableStat(masterTable);
                    tableStat.update(sqlType, sql, queryResult.getStartTime(), queryResult.getEndTime(), relationTables);
                }
                break;
            default:
                break;

        }
    }

    private TableStat getTableStat(String tableName) {
        TableStat userStat = tableStatMap.get(tableName);
        if (userStat == null) {
            if (lock.tryLock()) {
                try {
                    userStat = new TableStat(tableName);
                    tableStatMap.put(tableName, userStat);
                } finally {
                    lock.unlock();
                }
            } else {
                while (userStat == null) {
                    userStat = tableStatMap.get(tableName);
                }
            }
        }
        return userStat;
    }

    public Map<String, TableStat> getTableStatMap() {
        Map<String, TableStat> map = new LinkedHashMap<>(tableStatMap.size());
        map.putAll(tableStatMap);
        return map;
    }

    public List<TableStat> getTableStats(boolean isClear) {
        SortedSet<TableStat> tableStatSortedSet = new TreeSet<>(tableStatMap.values());
        List<TableStat> list = new ArrayList<>(tableStatSortedSet);
        if (isClear) {
            tableStatMap = new ConcurrentHashMap<>();
        }
        return list;
    }

    public void clearTable() {
        tableStatMap.clear();
    }


    /**
     * PARSER table name
     */
    private static class SQLParser {

        private SQLStatement parseStmt(String sql) {
            SQLStatementParser statParser = SQLParserUtils.createSQLStatementParser(sql, "mysql");
            SQLStatement stmt = statParser.parseStatement();
            return stmt;
        }

        /**
         * fix SCHEMA,`
         *
         * @param tableName
         * @return
         */
        private String fixName(String tableName) {
            if (tableName != null) {
                tableName = tableName.replace("`", "");
                int dotIdx = tableName.indexOf(".");
                if (dotIdx > 0) {
                    tableName = tableName.substring(1 + dotIdx).trim();
                }
            }
            return tableName;
        }

        /**
         * PARSER SQL table name
         */
        public List<String> parseTableNames(String sql) {
            final List<String> tables = new ArrayList<>();
            try {

                SQLStatement stmt = parseStmt(sql);
                if (stmt instanceof SQLReplaceStatement) {
                    String table = ((SQLReplaceStatement) stmt).getTableName().getSimpleName();
                    tables.add(fixName(table));

                } else if (stmt instanceof SQLInsertStatement) {
                    String table = ((SQLInsertStatement) stmt).getTableName().getSimpleName();
                    tables.add(fixName(table));

                } else if (stmt instanceof SQLUpdateStatement) {
                    String table = ((SQLUpdateStatement) stmt).getTableName().getSimpleName();
                    tables.add(fixName(table));

                } else if (stmt instanceof SQLDeleteStatement) {
                    String table = ((SQLDeleteStatement) stmt).getTableName().getSimpleName();
                    tables.add(fixName(table));

                } else if (stmt instanceof SQLSelectStatement) {

                    String dbType = stmt.getDbType();
                    if (!StringUtil.isEmpty(dbType) && JdbcConstants.MYSQL.equals(dbType)) {
                        stmt.accept(new MySqlASTVisitorAdapter() {
                            public boolean visit(SQLExprTableSource x) {
                                tables.add(fixName(x.toString()));
                                return super.visit(x);
                            }
                        });

                    } else {
                        stmt.accept(new SQLASTVisitorAdapter() {
                            public boolean visit(SQLExprTableSource x) {
                                tables.add(fixName(x.toString()));
                                return super.visit(x);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                LOGGER.info("TableStatAnalyzer err:" + e.toString());
            }

            return tables;
        }
    }

}
