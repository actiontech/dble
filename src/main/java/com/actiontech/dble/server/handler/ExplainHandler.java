/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.*;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.DirectGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.groupby.OrderedGroupByHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.JoinHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.join.NotInHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * @author mycat
 */
public final class ExplainHandler {
    private ExplainHandler() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ExplainHandler.class);
    private static final int FIELD_COUNT = 3;
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];

    static {
        FIELDS[0] = PacketUtil.getField("DATA_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[1] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[2] = PacketUtil.getField("SQL/REF", Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(String stmt, ServerConnection c, int offset) {
        stmt = stmt.substring(offset).trim();

        RouteResultset rrs = getRouteResultset(c, stmt);
        if (rrs == null) {
            return;
        }

        ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        byte packetId = header.getPacketId();
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, c, true);
        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, c, true);
        if (!rrs.isNeedOptimizer()) {
            // write rows
            for (RouteResultsetNode node : rrs.getNodes()) {
                RowDataPacket row = getRow(node, c.getCharset().getResults());
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        } else {
            List<String[]> results = getComplexQueryResult(rrs, c);
            for (String[] result : results) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(result[0], c.getCharset().getResults()));
                row.add(StringUtil.encode(result[1], c.getCharset().getResults()));
                row.add(StringUtil.encode(result[2].replaceAll("[\\t\\n\\r]", " "), c.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }

    private static List<String[]> getComplexQueryResult(RouteResultset rrs, ServerConnection c) {
        List<String[]> result = new ArrayList<>();
        DMLResponseHandler endHandler = buildNode(rrs, c);
        Map<DMLResponseHandler, RefHandlerInfo> handlerMap = new HashMap<>();
        Map<String, RefHandlerInfo> refMap = new HashMap<>();
        int mergeCnt = 0;
        Map<String, Integer> nameMap = new HashMap<>();
        String rootName = null;
        for (DMLResponseHandler startHandler : endHandler.getMerges()) {
            MultiNodeMergeHandler mergeHandler = (MultiNodeMergeHandler) startHandler;
            List<BaseSelectHandler> mergelist = new ArrayList<>();
            mergelist.addAll(((MultiNodeMergeHandler) startHandler).getExeHandlers());
            mergeCnt++;
            String mergeNode = "merge." + mergeCnt;
            RefHandlerInfo refInfo = new RefHandlerInfo(mergeNode, "MERGE");
            handlerMap.put(mergeHandler, refInfo);
            refMap.put(mergeNode, refInfo);
            for (BaseSelectHandler exeHandler : mergelist) {
                RouteResultsetNode rrss = exeHandler.getRrss();
                String dateNode = rrss.getName() + "." + rrss.getMultiplexNum();
                result.add(new String[]{dateNode, "BASE SQL", rrss.getStatement()});
                refInfo.addChild(dateNode);
            }
            String mergeRootName = dfsHandler(mergeHandler, handlerMap, refMap, nameMap);
            if (mergeCnt == 1) {
                if (mergeRootName == null) {
                    rootName = mergeNode;
                } else {
                    rootName = mergeRootName;
                }
            }
        }
        nameMap.clear();
        handlerMap.clear();
        Stack<RefHandlerInfo> stackSearch = new Stack<>();
        List<RefHandlerInfo> resultList = new ArrayList<>(refMap.size());
        stackSearch.push(refMap.get(rootName));
        while (stackSearch.size() > 0) {
            RefHandlerInfo root = stackSearch.pop();
            resultList.add(root);
            for (String child : root.getChildren()) {
                RefHandlerInfo childRef = refMap.get(child);
                if (childRef != null) {
                    stackSearch.push(childRef);
                }
            }
        }
        refMap.clear();
        for (int i = resultList.size() - 1; i >= 0; i--) {
            RefHandlerInfo handlerInfo = resultList.get(i);
            result.add(new String[]{handlerInfo.name, handlerInfo.type, handlerInfo.getChildrenNames()});
        }
        return result;
    }

    private static String dfsHandler(DMLResponseHandler handler, Map<DMLResponseHandler, RefHandlerInfo> handlerMap, Map<String, RefHandlerInfo> refMap, Map<String, Integer> nameMap) {
        DMLResponseHandler nextHandler = skipSendMake(handler.getNextHandler());
        String rootName = null;
        while (nextHandler != null) {
            RefHandlerInfo child = handlerMap.get(handler);
            String childName = child.name;
            String handlerType = getTypeName(nextHandler);
            if (!handlerMap.containsKey(nextHandler)) {
                String handlerName = genHandlerName(handlerType, nameMap);
                RefHandlerInfo handlerInfo = new RefHandlerInfo(handlerName, handlerType);
                handlerMap.put(nextHandler, handlerInfo);
                refMap.put(handlerName, handlerInfo);
                handlerInfo.addChild(childName);
                rootName = handlerName;
            } else {
                handlerMap.get(nextHandler).addChild(childName);
            }
            handler = nextHandler;
            nextHandler = skipSendMake(nextHandler.getNextHandler());
        }
        return rootName;
    }

    private static DMLResponseHandler skipSendMake(DMLResponseHandler handler) {
        while (handler instanceof SendMakeHandler) {
            handler = handler.getNextHandler();
        }
        return handler;
    }

    private static String genHandlerName(String handlerType, Map<String, Integer> nameMap) {
        String handlerName;
        if (nameMap.containsKey(handlerType)) {
            int number = nameMap.get(handlerType) + 1;
            nameMap.put(handlerType, number);
            handlerName = handlerType.toLowerCase() + "." + number;
        } else {
            nameMap.put(handlerType, 1);
            handlerName = handlerType.toLowerCase() + ".1";
        }
        return handlerName;
    }

    private static String getTypeName(DMLResponseHandler handler) {
        if (handler instanceof OrderedGroupByHandler) {
            return "ORDERED_GROUP";
        } else if (handler instanceof DistinctHandler) {
            return "DISTINCT";
        } else if (handler instanceof LimitHandler) {
            return "LIMIT";
        } else if (handler instanceof WhereHandler) {
            return "WHERE_FILTER";
        } else if (handler instanceof HavingHandler) {
            return "HAVING_FILTER";
        } else if (handler instanceof SendMakeHandler) {
            return "RENAME";
        } else if (handler instanceof UnionHandler) {
            return "UNION_ALL";
        } else if (handler instanceof OrderByHandler) {
            return "ORDER";
        } else if (handler instanceof NotInHandler) {
            return "NOT_IN";
        } else if (handler instanceof JoinHandler) {
            return "JOIN";
        } else if (handler instanceof DirectGroupByHandler) {
            return "DIRECT_GROUP";
        } else if (handler instanceof TempTableHandler) {
            return "NEST_LOOP";
        }
        return "OTHER";
    }

    private static DMLResponseHandler buildNode(RouteResultset rrs, ServerConnection c) {
        SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(c.getSchema(), c.getCharset().getResultsIndex());
        visitor.visit(ast);
        PlanNode node = visitor.getTableNode();
        node.setSql(rrs.getStatement());
        node.setUpFields();
        node = MyOptimizer.optimize(node);
        HandlerBuilder builder = new HandlerBuilder(node, c.getSession2());
        return builder.buildNode(c.getSession2(), node);
    }

    private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(node.getName(), charset));
        row.add(StringUtil.encode("BASE SQL", charset));
        row.add(StringUtil.encode(node.getStatement().replaceAll("[\\t\\n\\r]", " "), charset));
        return row;
    }

    private static RouteResultset getRouteResultset(ServerConnection c,
                                                    String stmt) {
        String db = c.getSchema();
        int sqlType = ServerParse.parse(stmt) & 0xff;
        if (db == null) {
            //TODO: EXPLAIN SCHEMA.TABLE
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return null;
        }
        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return null;
        }
        try {
            if (ServerParse.INSERT == sqlType && isInsertSeq(c, stmt, schema)) {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "insert sql using sequence,the explain result depends by sequence");
                return null;
            }
            return DbleServer.getInstance().getRouterService().route(schema, sqlType, stmt, c);
        } catch (Exception e) {
            if (e instanceof SQLException && !(e instanceof SQLNonTransientException)) {
                SQLException sqle = (SQLException) e;
                StringBuilder s = new StringBuilder();
                LOGGER.warn(s.append(c).append(stmt).toString() + " error:" + sqle);
                String msg = sqle.getMessage();
                c.writeErrMessage(sqle.getErrorCode(), msg == null ? sqle.getClass().getSimpleName() : msg);
                return null;
            } else {
                StringBuilder s = new StringBuilder();
                LOGGER.warn(s.append(c).append(stmt).toString() + " error:" + e);
                String msg = e.getMessage();
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
                return null;
            }
        }
    }

    private static boolean isInsertSeq(ServerConnection c, String stmt, SchemaConfig schema) throws SQLException {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        MySqlInsertStatement statement = (MySqlInsertStatement) parser.parseStatement();
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = statement.getTableSource();
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(c.getUser(), schemaName, tableSource);
        String tableName = schemaInfo.getTable();
        schema = schemaInfo.getSchemaConfig();
        TableConfig tableConfig = schema.getTables().get(tableName);
        if (tableConfig == null) {
            return false;
        } else if (tableConfig.isAutoIncrement()) {
            return true;
        }
        return false;
    }

    private static class RefHandlerInfo {
        private String name;
        private String type;
        private Set<String> children = new TreeSet<>();

        RefHandlerInfo(String name, String type) {
            this.name = name;
            this.type = type;
        }

        String getChildrenNames() {
            StringBuilder names = new StringBuilder("");
            int i = 0;
            for (String child : children) {
                if (i > 0) {
                    names.append(", ");
                }
                names.append(child);
                i++;
            }
            return names.toString();
        }

        public Set<String> getChildren() {
            return children;
        }

        void addChild(String child) {
            this.children.add(child);
        }
    }
}
