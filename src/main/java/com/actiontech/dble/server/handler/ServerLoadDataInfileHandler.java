/*
 * Copyright (C) 2016-2019 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.net.handler.LoadDataInfileHandler;
import com.actiontech.dble.net.mysql.BinaryPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RequestFilePacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.actiontech.dble.util.ObjectUtil;
import com.actiontech.dble.util.SqlStringUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * mysql client need add --local-infile=1
 * CHARACTER SET 'gbk' in load data sql  the charset need ', otherwise the druid will error
 */
public final class ServerLoadDataInfileHandler implements LoadDataInfileHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerLoadDataInfileHandler.class);
    //innodb limit of columns per table, https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html
    private static final int DEFAULT_MAX_COLUMNS = 1017;
    private ServerConnection serverConnection;
    private String sql;
    private String fileName;
    private byte packID = 0;
    private MySqlLoadDataInFileStatement statement;

    private Map<String, LoadData> routeResultMap = new HashMap<>();

    private LoadData loadData;
    private ByteArrayOutputStream tempByteBuffer;
    private long tempByteBufferSize = 0;
    private String tempPath;
    private String tempFile;
    private boolean isHasStoreToFile = false;

    private SchemaConfig schema;
    private final SystemConfig systemConfig = DbleServer.getInstance().getConfig().getSystem();
    private String tableName;
    private TableConfig tableConfig;
    private int partitionColumnIndex = -1;
    private int autoIncrementIndex = -1;
    private boolean appendAutoIncrementColumn = false;
    private LayerCachePool tableId2DataNodeCache;
    private boolean isStartLoadData = false;

    public int getPackID() {
        return packID;
    }

    public void setPackID(byte packID) {
        this.packID = packID;
    }

    public ServerLoadDataInfileHandler(ServerConnection serverConnection) {
        this.serverConnection = serverConnection;

    }

    private static String parseFileName(String sql) {
        String uSql = sql.toUpperCase();
        int index0 = uSql.indexOf("INFILE");

        for (int i = index0 + 6; i < sql.length(); i++) {
            char quoteChar = sql.charAt(i);
            if (quoteChar > 0x0020) {
                String quoteStr = String.valueOf(quoteChar);
                return sql.substring(i + 1, sql.indexOf(quoteStr, i + 1));
            }
        }
        return null;
    }

    private void parseLoadDataPram() {
        loadData = new LoadData();
        SQLTextLiteralExpr rawLineEnd = (SQLTextLiteralExpr) statement.getLinesTerminatedBy();
        String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.getText();
        loadData.setLineTerminatedBy(lineTerminatedBy);

        SQLTextLiteralExpr rawFieldEnd = (SQLTextLiteralExpr) statement.getColumnsTerminatedBy();
        String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.getText();
        loadData.setFieldTerminatedBy(fieldTerminatedBy);

        SQLTextLiteralExpr rawEnclosed = (SQLTextLiteralExpr) statement.getColumnsEnclosedBy();
        String enclose = ((rawEnclosed == null) || rawEnclosed.getText().isEmpty()) ? null : rawEnclosed.getText();
        loadData.setEnclose(enclose);

        SQLTextLiteralExpr escapedExpr = (SQLTextLiteralExpr) statement.getColumnsEscaped();
        String escaped = escapedExpr == null ? "\\" : escapedExpr.getText();
        loadData.setEscape(escaped);
        String charset = statement.getCharset() != null ? statement.getCharset() : DbleServer.getInstance().getSystemVariables().getDefaultValue("character_set_database");
        loadData.setCharset(CharsetUtil.getJavaCharset(charset));
        loadData.setFileName(fileName);
    }

    @Override
    public void start(String strSql) {
        clear();
        this.sql = strSql;

        if (this.checkPartition(strSql)) {
            serverConnection.writeErrMessage(ErrorCode.ER_UNSUPPORTED_PS, " unsupported load data with Partition");
            clear();
            return;
        }

        try {
            statement = (MySqlLoadDataInFileStatement) new MySqlStatementParser(strSql).parseStatement();
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(serverConnection.getUser(), serverConnection.getSchema(), statement.getTableName(), null);
            tableName = schemaInfo.getTable();
            schema = schemaInfo.getSchemaConfig();
        } catch (SQLException e) {
            clear();
            serverConnection.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        }

        // if there are schema in sql, remove it.
        if (statement.getTableName() instanceof SQLPropertyExpr) {
            statement.setTableName(new SQLIdentifierExpr(tableName));
        }

        tableConfig = schema.getTables().get(tableName);
        if (!DbleServer.getInstance().getTmManager().checkTableExists(schema.getName(), tableName)) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' or table mata doesn't exist";
            clear();
            serverConnection.writeErrMessage("42S02", msg, ErrorCode.ER_NO_SUCH_TABLE);
            return;
        }

        fileName = parseFileName(strSql);
        if (fileName == null) {
            serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, " file name is null !");
            clear();
            return;
        }

        tableId2DataNodeCache = (LayerCachePool) DbleServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
        tempPath = SystemConfig.getHomePath() + File.separator + "temp" + File.separator + serverConnection.getId() + File.separator;
        tempFile = tempPath + "clientTemp.txt";
        tempByteBuffer = new ByteArrayOutputStream();

        if (!trySetPartitionOrAutoIncrementColumnIndex(statement)) {
            return;
        }

        if (tableConfig != null && tableConfig.isAutoIncrement() && autoIncrementIndex == -1) {
            final String incrementColumn = tableConfig.getTrueIncrementColumn();
            statement.getColumns().add(new SQLIdentifierExpr(incrementColumn));
            autoIncrementIndex = statement.getColumns().size() - 1;
            appendAutoIncrementColumn = true;
            sql = SQLUtils.toMySqlString(statement);
            if (incrementColumn.equalsIgnoreCase(getPartitionColumn())) {
                partitionColumnIndex = autoIncrementIndex;
            }
        }

        parseLoadDataPram();
        if (statement.isLocal()) {
            isStartLoadData = true;
            //request file from client
            ByteBuffer buffer = serverConnection.allocate();
            RequestFilePacket filePacket = new RequestFilePacket();
            filePacket.setFileName(fileName.getBytes());
            filePacket.setPacketId(1);
            filePacket.write(buffer, serverConnection, true);
        } else {
            if (!new File(fileName).exists()) {
                String msg = fileName + " is not found!";
                clear();
                serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, msg);
            } else {
                if (parseFileByLine(fileName, loadData.getCharset(), loadData.getLineTerminatedBy())) {
                    RouteResultset rrs = buildResultSet(routeResultMap);
                    if (rrs != null) {
                        flushDataToFile();
                        isStartLoadData = false;
                        serverConnection.getSession2().execute(rrs);
                    }
                }
            }
        }
    }

    @Override
    public void handle(byte[] data) {
        try {
            if (sql == null) {
                clear();
                serverConnection.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                return;
            }
            BinaryPacket packet = new BinaryPacket();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data, 0, data.length);
            packet.read(inputStream);

            saveByteOrToFile(packet.getData(), false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * findout the index of the partition key
     */
    private boolean trySetPartitionOrAutoIncrementColumnIndex(MySqlLoadDataInFileStatement sqlStatement) {
        if (tableConfig != null) {
            List<SQLExpr> columns = sqlStatement.getColumns();
            String pColumn = getPartitionColumn();
            boolean autoIncrement = tableConfig.isAutoIncrement();
            if (pColumn != null || autoIncrement) {
                String incrementColumn = tableConfig.getTrueIncrementColumn();
                if (columns != null && columns.size() > 0) {
                    for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
                        String column = StringUtil.removeBackQuote(columns.get(i).toString());
                        if (column.equalsIgnoreCase(pColumn)) {
                            partitionColumnIndex = i;
                        }
                        if (autoIncrement && column.equalsIgnoreCase(incrementColumn)) {
                            autoIncrementIndex = i;
                        }
                    }
                } else {
                    try {
                        StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schema.getName(), tableName);
                        if (tbMeta != null) {
                            for (int i = 0; i < tbMeta.getColumnsCount(); i++) {
                                String column = tbMeta.getColumns(i).getName();
                                if (column.equalsIgnoreCase(pColumn)) {
                                    partitionColumnIndex = i;
                                }
                                if (autoIncrement && column.equalsIgnoreCase(incrementColumn)) {
                                    autoIncrementIndex = i;
                                }
                            }
                        }
                    } catch (Exception e) {
                        serverConnection.writeErrMessage(ErrorCode.ER_DOING_DDL, " table is doing DDL or table meta error");
                        clear();
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private synchronized void saveByteOrToFile(byte[] data, boolean isForce) {
        if (data != null) {
            tempByteBufferSize = tempByteBufferSize + data.length;
            try {
                tempByteBuffer.write(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if ((isForce && isHasStoreToFile) || tempByteBufferSize > 200 * 1024 * 1024) { //200M
            FileOutputStream channel = null;
            try {
                File file = new File(tempFile);
                Files.createParentDirs(file);
                channel = new FileOutputStream(file, true);

                tempByteBuffer.writeTo(channel);
                tempByteBuffer = new ByteArrayOutputStream();
                tempByteBufferSize = 0;
                isHasStoreToFile = true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (channel != null) {
                        channel.close();
                    }
                } catch (IOException ignored) {
                    //ignore error
                }
            }
        }
    }

    private RouteResultset tryDirectRoute(String strSql, String[] lineList) {
        RouteResultset rrs = new RouteResultset(strSql, ServerParse.INSERT);
        rrs.setLoadData(true);
        if (tableConfig != null && tableConfig.isGlobalTable()) {
            ArrayList<String> dataNodes = tableConfig.getDataNodes();
            RouteResultsetNode[] rrsNodes = new RouteResultsetNode[dataNodes.size()];
            for (int i = 0, dataNodesSize = dataNodes.size(); i < dataNodesSize; i++) {
                String dataNode = dataNodes.get(i);
                RouteResultsetNode rrNode = new RouteResultsetNode(dataNode, ServerParse.INSERT, strSql);
                rrsNodes[i] = rrNode;
            }
            rrs.setGlobalTable(true);
            rrs.setNodes(rrsNodes);
            return rrs;
        } else {
            DruidShardingParseInfo ctx = new DruidShardingParseInfo();
            ctx.addTable(tableName);

            if (partitionColumnIndex != -1) {
                if (lineList.length < partitionColumnIndex + 1 || StringUtil.isEmpty(lineList[partitionColumnIndex])) {
                    throw new RuntimeException("Partition column is empty in line '" + StringUtil.join(lineList, loadData.getFieldTerminatedBy()) + "'");
                }
                RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
                routeCalculateUnit.addShardingExpr(tableName, getPartitionColumn(),
                        parseFieldString(lineList[partitionColumnIndex], loadData.getEnclose(), loadData.getEscape()));
                ctx.addRouteCalculateUnit(routeCalculateUnit);
            }

            try {
                SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
                if (ctx.getRouteCalculateUnits().size() > 0) {
                    for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
                        RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, unit, rrs, false, tableId2DataNodeCache, null);
                        if (rrsTmp != null) {
                            Collections.addAll(nodeSet, rrsTmp.getNodes());
                        }
                    }
                } else {
                    RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, null, rrs, false, tableId2DataNodeCache, null);
                    if (rrsTmp != null) {
                        Collections.addAll(nodeSet, rrsTmp.getNodes());
                    }
                }

                RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
                int i = 0;
                for (RouteResultsetNode aNodeSet : nodeSet) {
                    nodes[i] = aNodeSet;
                    i++;
                }

                rrs.setNodes(nodes);
                return rrs;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }


    private void parseOneLine(String[] line, boolean toFile) throws Exception {
        if (loadData.getEnclose() != null && loadData.getEnclose().charAt(0) > 0x0020) {
            for (int i = 0; i < line.length; i++) {
                line[i] = line[i].trim();
            }
        }

        if (autoIncrementIndex != -1) {
            line = rebuildRow(line);
        }

        RouteResultset rrs = tryDirectRoute(sql, line);

        if (rrs == null || rrs.getNodes() == null || rrs.getNodes().length == 0) {
            //do nothing
            throw new Exception("record " + StringUtil.join(line, loadData.getLineTerminatedBy()) + "has no route result");
        } else {
            for (RouteResultsetNode routeResultsetNode : rrs.getNodes()) {
                String name = routeResultsetNode.getName();
                LoadData data = routeResultMap.get(name);
                if (data == null) {
                    data = new LoadData();
                    data.setCharset(loadData.getCharset());
                    data.setEnclose(loadData.getEnclose());
                    data.setFieldTerminatedBy(loadData.getFieldTerminatedBy());
                    data.setLineTerminatedBy(loadData.getLineTerminatedBy());
                    data.setEscape(loadData.getEscape());
                    routeResultMap.put(name, data);
                }

                String jLine = joinField(line, data);
                if (data.getData() == null) {
                    data.setData(Lists.newArrayList(jLine));
                } else {
                    data.getData().add(jLine);
                }

                if (toFile && data.getData().size() > systemConfig.getMaxRowSizeToFile()) {
                    //avoid OOM
                    saveDataToFile(data, name);
                }
            }
        }
    }

    private String[] rebuildRow(String[] line) throws Exception {
        if (autoIncrementIndex >= line.length) {
            autoIncrementIndex = line.length;
            String[] newLine = new String[line.length + 1];
            System.arraycopy(line, 0, newLine, 0, line.length);
            String tableKey = StringUtil.getFullName(schema.getName(), tableName);
            newLine[line.length] = String.valueOf(DbleServer.getInstance().getSequenceHandler().nextId(tableKey));
            line = newLine;
        } else {
            if (StringUtil.isEmpty(line[autoIncrementIndex])) {
                String tableKey = StringUtil.getFullName(schema.getName(), tableName);
                line[autoIncrementIndex] = String.valueOf(DbleServer.getInstance().getSequenceHandler().nextId(tableKey));
            } else if (!appendAutoIncrementColumn) {
                throw new Exception("you can't set value for Autoincrement column!");
            }
        }
        return line;
    }

    private void flushDataToFile() {
        for (Map.Entry<String, LoadData> stringLoadDataEntry : routeResultMap.entrySet()) {
            LoadData value = stringLoadDataEntry.getValue();
            if (value.getFileName() != null && value.getData() != null && value.getData().size() > 0) {
                saveDataToFile(value, stringLoadDataEntry.getKey());
            }
        }
    }

    private void saveDataToFile(LoadData data, String dnName) {
        if (data.getFileName() == null) {
            String dnPath = tempPath + dnName + ".txt";
            data.setFileName(dnPath);
        }

        File dnFile = new File(data.getFileName());
        try {
            if (!dnFile.exists()) {
                Files.createParentDirs(dnFile);
            }
            Files.append(joinLine(data.getData(), data), dnFile, Charset.forName(loadData.getCharset()));

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            data.setData(null);
        }
    }


    private String joinLine(List<String> data, LoadData loaddata) {
        StringBuilder sb = new StringBuilder();
        for (String s : data) {
            sb.append(s).append(loaddata.getLineTerminatedBy());
        }
        return sb.toString();
    }


    private String joinField(String[] src, LoadData loaddata) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, srcLength = src.length; i < srcLength; i++) {
            String s = src[i] != null ? src[i] : "";
            sb.append(s);
            if (i != srcLength - 1) {
                sb.append(loaddata.getFieldTerminatedBy());
            }
        }

        return sb.toString();
    }


    private RouteResultset buildResultSet(Map<String, LoadData> routeMap) {
        if (routeMap.size() == 0) {
            return null;
        }
        statement.setLocal(true);
        SQLLiteralExpr fn = new SQLCharExpr(fileName);    //druid will filter path, reset it now
        statement.setFileName(fn);
        //replace IGNORE X LINES in SQL to avoid  IGNORING X LINE in every node.
        String srcStatement = this.ignoreLinesDelete(SqlStringUtil.toSQLString(statement));
        RouteResultset rrs = new RouteResultset(srcStatement, ServerParse.LOAD_DATA_INFILE_SQL);
        rrs.setLoadData(true);
        rrs.setStatement(srcStatement);
        rrs.setFinishedRoute(true);
        rrs.setGlobalTable(tableConfig == null ? false : this.tableConfig.isGlobalTable());
        int size = routeMap.size();
        RouteResultsetNode[] routeResultsetNodes = new RouteResultsetNode[size];
        int index = 0;
        for (Map.Entry<String, LoadData> entry : routeMap.entrySet()) {
            RouteResultsetNode rrNode = new RouteResultsetNode(entry.getKey(), ServerParse.LOAD_DATA_INFILE_SQL, srcStatement);
            rrNode.setStatement(srcStatement);
            LoadData newLoadData = new LoadData();
            ObjectUtil.copyProperties(loadData, newLoadData);
            newLoadData.setLocal(true);
            LoadData loadData1 = entry.getValue();
            if (loadData1.getFileName() != null) {
                newLoadData.setFileName(loadData1.getFileName());
            } else {
                newLoadData.setData(loadData1.getData());
            }
            rrNode.setLoadData(newLoadData);

            routeResultsetNodes[index] = rrNode;
            index++;
        }
        rrs.setNodes(routeResultsetNodes);
        return rrs;
    }


    private String parseFieldString(String value, String enclose, String escape) {
        //avoid null point execption
        if (value == null) {
            return value;
        }

        //if the value is cover by enclose char and enclose char is not null, clear the enclose char.
        if (enclose != null && !"".equals(enclose) && (value.startsWith(enclose) && value.endsWith(enclose))) {
            return this.escaped(value.substring(enclose.length() - 1, value.length() - enclose.length()).replace("\\", "\\\\").replace(escape, "\\"));
        }
        //else replace escape because \ is used as escape in insert.
        return this.escaped(value.replace("\\", "\\\\").replace(escape, "\\"));
    }


    private String escaped(String input) {
        StringBuilder output = new StringBuilder();
        char[] x = input.toCharArray();
        for (int i = 0; i < x.length; i++) {
            if (x[i] == '\\' && i < x.length - 1) {
                switch (x[i + 1]) {
                    case 'b':
                        output.append('\b');
                        break;
                    case 't':
                        output.append('\t');
                        break;
                    case 'n':
                        output.append('\n');
                        break;
                    case 'f':
                        output.append('\f');
                        break;
                    case 'r':
                        output.append('\r');
                        break;
                    case '"':
                        output.append('\"');
                        break;
                    case '\'':
                        output.append('\'');
                        break;
                    case '\\':
                        output.append('\\');
                        break;
                    default:
                        output.append(x[i]);
                }
                i++;
                continue;
            }
            output.append(x[i]);
        }
        return output.toString();
    }


    @Override
    public void end(byte packId) {
        isStartLoadData = false;
        this.packID = packId;
        //empty packet for end
        saveByteOrToFile(null, true);

        if (isHasStoreToFile) {
            parseFileByLine(tempFile, loadData.getCharset(), loadData.getLineTerminatedBy());
        } else {
            String content = new String(tempByteBuffer.toByteArray(), Charset.forName(loadData.getCharset()));
            if ("".equals(content)) {
                clear();
                OkPacket ok = new OkPacket();
                ok.setPacketId(++packId);
                ok.setMessage("Records: 0  Deleted: 0  Skipped: 0  Warnings: 0".getBytes());
                ok.write(serverConnection);
                return;
            }
            // List<String> lines = Splitter.on(loadData.getLineTerminatedBy()).omitEmptyStrings().splitToList(content);
            CsvParserSettings settings = new CsvParserSettings();
            settings.setMaxColumns(DEFAULT_MAX_COLUMNS);
            settings.setMaxCharsPerColumn(systemConfig.getMaxCharsPerColumn());
            settings.getFormat().setLineSeparator(loadData.getLineTerminatedBy());
            settings.getFormat().setDelimiter(loadData.getFieldTerminatedBy().charAt(0));
            settings.getFormat().setComment('\0');
            if (loadData.getEnclose() != null) {
                settings.getFormat().setQuote(loadData.getEnclose().charAt(0));
            } else {
                settings.getFormat().setQuote('\0');
            }
            if (loadData.getEscape() != null) {
                settings.getFormat().setQuoteEscape(loadData.getEscape().charAt(0));
            }
            settings.getFormat().setNormalizedNewline(loadData.getLineTerminatedBy().charAt(0));
            settings.setSkipEmptyLines(false);
            settings.trimValues(false);

            CsvParser parser = new CsvParser(settings);
            try {
                parser.beginParsing(new StringReader(content));
                String[] row = null;

                int ignoreNumber = 0;
                if (statement.getIgnoreLinesNumber() != null && !"".equals(statement.getIgnoreLinesNumber().toString())) {
                    ignoreNumber = Integer.parseInt(statement.getIgnoreLinesNumber().toString());
                }
                while ((row = parser.parseNext()) != null) {
                    if (ignoreNumber == 0) {
                        if ((row.length == 1 && row[0] == null) || row.length == 0) {
                            continue;
                        }
                        try {
                            parseOneLine(row, true);
                        } catch (Exception e) {
                            clear();
                            serverConnection.writeErrMessage(++packId, ErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW, "row data can't not calculate a sharding value," + e.getMessage());
                            return;
                        }
                    } else {
                        ignoreNumber--;
                    }
                }
            } finally {
                parser.stopParsing();
            }
        }

        RouteResultset rrs = buildResultSet(routeResultMap);
        if (rrs != null) {
            flushDataToFile();
            serverConnection.getSession2().execute(rrs);
        }
    }


    private boolean parseFileByLine(String file, String encode, String split) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setMaxColumns(DEFAULT_MAX_COLUMNS);
        settings.setMaxCharsPerColumn(systemConfig.getMaxCharsPerColumn());
        settings.getFormat().setLineSeparator(loadData.getLineTerminatedBy());
        settings.getFormat().setDelimiter(loadData.getFieldTerminatedBy().charAt(0));
        settings.getFormat().setComment('\0');
        if (loadData.getEnclose() != null) {
            settings.getFormat().setQuote(loadData.getEnclose().charAt(0));
        } else {
            settings.getFormat().setQuote('\0');
        }
        settings.getFormat().setNormalizedNewline(loadData.getLineTerminatedBy().charAt(0));

        settings.trimValues(false);

        CsvParser parser = new CsvParser(settings);
        InputStreamReader reader = null;
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
            reader = new InputStreamReader(fileInputStream, encode);
            parser.beginParsing(reader);
            String[] row = null;

            int ignoreNumber = 0;
            if (statement.getIgnoreLinesNumber() != null && !"".equals(statement.getIgnoreLinesNumber().toString())) {
                ignoreNumber = Integer.parseInt(statement.getIgnoreLinesNumber().toString());
            }
            boolean empty = true;
            while ((row = parser.parseNext()) != null) {
                if (ignoreNumber == 0) {
                    if ((row.length == 1 && row[0] == null) || row.length == 0) {
                        continue;
                    }
                    try {
                        parseOneLine(row, true);
                    } catch (Exception e) {
                        clear();
                        serverConnection.writeErrMessage(ErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW, "row data can't not calculate a sharding value," + e.getMessage());
                        return false;
                    }
                    empty = false;
                } else {
                    ignoreNumber--;
                }
            }
            if (empty) {
                byte packId = packID;
                clear();
                OkPacket ok = new OkPacket();
                ok.setPacketId(++packId);
                ok.setMessage("Records: 0  Deleted: 0  Skipped: 0  Warnings: 0".getBytes());
                ok.write(serverConnection);
                return false;
            }
            return true;
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            parser.stopParsing();
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    /**
     * check if the sql is contain the partition. If the sql contain the partition word then stopped.
     *
     * @param strSql
     * @throws Exception
     */
    private boolean checkPartition(String strSql) {
        Pattern p = Pattern.compile("PARTITION\\s{0,}([\\s\\S]*)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(strSql);
        return m.find();
    }


    /**
     * use a Regular Expression to replace the "IGNORE    1234 LINES" to the " "
     *
     * @param strSql
     * @return
     */
    private String ignoreLinesDelete(String strSql) {
        Pattern p = Pattern.compile("IGNORE\\s{0,}\\d{0,}\\s{0,}LINES", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(strSql);
        StringBuffer sb = new StringBuffer();
        if (m.find()) {
            m.appendReplacement(sb, " ");
        } else {
            return strSql;
        }
        m.appendTail(sb);
        return sb.toString();
    }


    public void clear() {
        isStartLoadData = false;
        tableId2DataNodeCache = null;
        schema = null;
        tableConfig = null;
        isHasStoreToFile = false;
        packID = 0;
        tempByteBufferSize = 0;
        tableName = null;
        partitionColumnIndex = -1;
        autoIncrementIndex = -1;
        appendAutoIncrementColumn = false;
        if (tempFile != null) {
            File temp = new File(tempFile);
            if (temp.exists()) {
                temp.delete();
            }
        }
        if (tempPath != null && new File(tempPath).exists()) {
            deleteFile(tempPath);
        }
        tempByteBuffer = null;
        loadData = null;
        sql = null;
        fileName = null;
        statement = null;
        routeResultMap.clear();
    }


    @Override
    public byte getLastPackId() {
        return packID;
    }


    @Override
    public boolean isStartLoadData() {
        return isStartLoadData;
    }


    private String getPartitionColumn() {
        String pColumn;
        if (tableConfig.getParentTC() != null) {
            pColumn = tableConfig.getJoinKey();
        } else {
            pColumn = tableConfig.getPartitionColumn();
        }
        return pColumn;
    }


    /**
     * deleteFile and its children
     *
     * @param dirPath
     * @throws Exception
     */
    private static void deleteFile(String dirPath) {
        File fileDirToDel = new File(dirPath);
        if (!fileDirToDel.exists()) {
            return;
        }
        if (fileDirToDel.isFile()) {
            fileDirToDel.delete();
            return;
        }
        File[] fileList = fileDirToDel.listFiles();
        if (fileList != null) {
            for (File file : fileList) {
                if (file.isFile() && file.exists()) {
                    boolean delete = file.delete();
                } else if (file.isDirectory()) {
                    deleteFile(file.getAbsolutePath());
                    file.delete();
                }
            }
        }
        fileDirToDel.delete();
    }

}
