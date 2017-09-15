/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.net.handler.LoadDataInfileHandler;
import com.actiontech.dble.net.mysql.BinaryPacket;
import com.actiontech.dble.net.mysql.RequestFilePacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.SystemVariables;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.actiontech.dble.util.ObjectUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

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
    private ServerConnection serverConnection;
    private String sql;
    private String fileName;
    private byte packID = 0;
    private MySqlLoadDataInFileStatement statement;

    private Map<String, LoadData> routeResultMap = new HashMap<>();

    private LoadData loadData;
    private ByteArrayOutputStream tempByteBuffer;
    private long tempByteBuffrSize = 0;
    private String tempPath;
    private String tempFile;
    private boolean isHasStoreToFile = false;

    private SchemaConfig schema;
    private String tableName;
    private TableConfig tableConfig;
    private int partitionColumnIndex = -1;
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
        String usql = sql.toUpperCase();
        int index0 = usql.indexOf("INFILE");

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
        String enclose = rawEnclosed == null ? null : rawEnclosed.getText();
        loadData.setEnclose(enclose);

        SQLTextLiteralExpr escapseExpr = (SQLTextLiteralExpr) statement.getColumnsEscaped();
        String escapse = escapseExpr == null ? "\\" : escapseExpr.getText();
        loadData.setEscape(escapse);
        String charset = statement.getCharset() != null ? statement.getCharset() : SystemVariables.getDefaultValue("character_set_filesystem");
        loadData.setCharset(charset);
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

        SQLStatementParser parser = new MySqlStatementParser(strSql);
        statement = (MySqlLoadDataInFileStatement) parser.parseStatement();
        fileName = parseFileName(strSql);
        if (fileName == null) {
            serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, " file name is null !");
            clear();
            return;
        }

        schema = DbleServer.getInstance().getConfig().getSchemas().get(serverConnection.getSchema());
        tableId2DataNodeCache = (LayerCachePool) DbleServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
        tableName = statement.getTableName().getSimpleName();
        if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
            tableName = tableName.toLowerCase();
        }

        tableConfig = schema.getTables().get(tableName);
        tempPath = SystemConfig.getHomePath() + File.separator + "temp" + File.separator + serverConnection.getId() + File.separator;
        tempFile = tempPath + "clientTemp.txt";
        tempByteBuffer = new ByteArrayOutputStream();

        List<SQLExpr> columns = statement.getColumns();
        if (tableConfig != null) {
            String pColumn = getPartitionColumn();
            if (pColumn != null && columns != null && columns.size() > 0) {
                for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
                    String column = StringUtil.removeBackQuote(columns.get(i).toString());
                    if (pColumn.equalsIgnoreCase(column)) {
                        partitionColumnIndex = i;
                        break;
                    }
                }
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
                serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, fileName + " is not found!");
                clear();
            } else {
                parseFileByLine(fileName, loadData.getCharset(), loadData.getLineTerminatedBy());
                RouteResultset rrs = buildResultSet(routeResultMap);
                if (rrs != null) {
                    flushDataToFile();
                    isStartLoadData = false;
                    serverConnection.getSession2().execute(rrs, ServerParse.LOAD_DATA_INFILE_SQL);
                }
            }
        }
    }

    @Override
    public void handle(byte[] data) {
        try {
            if (sql == null) {
                serverConnection.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                clear();
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

    private synchronized void saveByteOrToFile(byte[] data, boolean isForce) {
        if (data != null) {
            tempByteBuffrSize = tempByteBuffrSize + data.length;
            try {
                tempByteBuffer.write(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if ((isForce && isHasStoreToFile) || tempByteBuffrSize > 200 * 1024 * 1024) { //200M
            FileOutputStream channel = null;
            try {
                File file = new File(tempFile);
                Files.createParentDirs(file);
                channel = new FileOutputStream(file, true);

                tempByteBuffer.writeTo(channel);
                tempByteBuffer = new ByteArrayOutputStream();
                tempByteBuffrSize = 0;
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
        if (tableConfig == null && schema.getDataNode() != null) {
            //default node
            RouteResultsetNode rrNode = new RouteResultsetNode(schema.getDataNode(), ServerParse.INSERT, strSql);
            rrs.setNodes(new RouteResultsetNode[]{rrNode});
            return rrs;
        } else if (tableConfig != null && tableConfig.isGlobalTable()) {
            ArrayList<String> dataNodes = tableConfig.getDataNodes();
            RouteResultsetNode[] rrsNodes = new RouteResultsetNode[dataNodes.size()];
            for (int i = 0, dataNodesSize = dataNodes.size(); i < dataNodesSize; i++) {
                String dataNode = dataNodes.get(i);
                RouteResultsetNode rrNode = new RouteResultsetNode(dataNode, ServerParse.INSERT, strSql);
                rrsNodes[i] = rrNode;
            }
            rrs.setNodes(rrsNodes);
            return rrs;
        } else if (tableConfig != null) {
            DruidShardingParseInfo ctx = new DruidShardingParseInfo();
            ctx.addTable(tableName);

            if (partitionColumnIndex == -1 || partitionColumnIndex >= lineList.length) {
                return null;
            } else {
                String value = lineList[partitionColumnIndex];
                RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
                routeCalculateUnit.addShardingExpr(tableName, getPartitionColumn(),
                                                   parseFieldString(value, loadData.getEnclose(), loadData.getEscape()));
                ctx.addRouteCalculateUnit(routeCalculateUnit);

                try {
                    SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
                    for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
                        RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, unit, rrs, false, tableId2DataNodeCache);
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

        return null;
    }


    private void parseOneLine(List<SQLExpr> columns, String table, String[] line, boolean toFile, String lineEnd) {
        if (loadData.getEnclose() != null && loadData.getEnclose().charAt(0) > 0x0020) {
            for (int i = 0; i < line.length; i++) {
                line[i] = line[i].trim();
            }
        }

        RouteResultset rrs = tryDirectRoute(sql, line);
        if (rrs == null || rrs.getNodes() == null || rrs.getNodes().length == 0) {
            String insertSql = makeSimpleInsert(columns, line, table);
            rrs = serverConnection.routeSQL(insertSql, ServerParse.INSERT);
        }

        if (rrs == null || rrs.getNodes() == null || rrs.getNodes().length == 0) {
            //do nothing
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

                if (toFile && data.getData().size() > 10000) {
                    //avoid OOM
                    saveDataToFile(data, name);
                }
            }
        }
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
        statement.setLocal(true);
        SQLLiteralExpr fn = new SQLCharExpr(fileName);    //druid will filter path, reset it now
        statement.setFileName(fn);
        //replace IGNORE X LINES in SQL to avoid  IGNORING X LINE in every node.
        String srcStatement = this.ignoreLinesDelete(statement.toString());
        RouteResultset rrs = new RouteResultset(srcStatement, ServerParse.LOAD_DATA_INFILE_SQL);
        rrs.setLoadData(true);
        rrs.setStatement(srcStatement);
        rrs.setFinishedRoute(true);
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


    private String makeSimpleInsert(List<SQLExpr> columns, String[] fields, String table) {
        StringBuilder sb = new StringBuilder();
        sb.append(LoadData.LOAD_DATA_HINT).append("insert into ").append(table.toUpperCase());
        if (columns != null && columns.size() > 0) {
            sb.append("(");
            for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
                SQLExpr column = columns.get(i);
                sb.append(column.toString());
                if (i != columnsSize - 1) {
                    sb.append(",");
                }
            }
            sb.append(") ");
        }

        sb.append(" values (");
        for (int i = 0, columnsSize = fields.length; i < columnsSize; i++) {
            String column = fields[i];

            sb.append("'").append(parseFieldString(column, loadData.getEnclose(), loadData.getEscape())).append("'");

            if (i != columnsSize - 1) {
                sb.append(",");
            }
        }
        sb.append(")");

        return sb.toString();
    }


    private String parseFieldString(String value, String encose, String escape) {
        //avoid null point execption
        if (value == null) {
            return value;
        }

        //if the value is cover by enclose char and enclose char is not null, clear the enclose char.
        if (encose != null && !"".equals(encose) && (value.startsWith(encose) && value.endsWith(encose))) {
            return this.escaped(value.substring(encose.length() - 1, value.length() - encose.length()).replace("\\", "\\\\").replace(escape, "\\"));
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
    public void end(byte packid) {
        isStartLoadData = false;
        this.packID = packid;
        //empty packet for end
        saveByteOrToFile(null, true);

        List<SQLExpr> columns = statement.getColumns();
        String tableSimpleName = statement.getTableName().getSimpleName();
        if (isHasStoreToFile) {
            parseFileByLine(tempFile, loadData.getCharset(), loadData.getLineTerminatedBy());
        } else {
            String content = new String(tempByteBuffer.toByteArray(), Charset.forName(loadData.getCharset()));

            // List<String> lines = Splitter.on(loadData.getLineTerminatedBy()).omitEmptyStrings().splitToList(content);
            CsvParserSettings settings = new CsvParserSettings();
            settings.setMaxColumns(65535);
            settings.setMaxCharsPerColumn(65535);
            settings.getFormat().setLineSeparator(loadData.getLineTerminatedBy());
            settings.getFormat().setDelimiter(loadData.getFieldTerminatedBy().charAt(0));
            if (loadData.getEnclose() != null) {
                settings.getFormat().setQuote(loadData.getEnclose().charAt(0));
            }
            if (loadData.getEscape() != null) {
                settings.getFormat().setQuoteEscape(loadData.getEscape().charAt(0));
            }
            settings.getFormat().setNormalizedNewline(loadData.getLineTerminatedBy().charAt(0));
            /*
             *  fix bug #1074 : LOAD DATA local INFILE导入的所有Boolean类型全部变成了false
             *  不可见字符将在CsvParser被当成whitespace过滤掉, 使用settings.trimValues(false)来避免被过滤掉
             *  FIXME : 设置trimValues(false)之后, 会引起字段值前后的空白字符无法被过滤!
             */
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
                        parseOneLine(columns, tableSimpleName, row, true, loadData.getLineTerminatedBy());
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
            serverConnection.getSession2().execute(rrs, ServerParse.LOAD_DATA_INFILE_SQL);
        }

        // sendOk(++packID);
    }


    private void parseFileByLine(String file, String encode, String split) {
        List<SQLExpr> columns = statement.getColumns();

        CsvParserSettings settings = new CsvParserSettings();
        settings.setMaxColumns(65535);
        settings.setMaxCharsPerColumn(65535);
        settings.getFormat().setLineSeparator(loadData.getLineTerminatedBy());
        settings.getFormat().setDelimiter(loadData.getFieldTerminatedBy().charAt(0));
        if (loadData.getEnclose() != null) {
            settings.getFormat().setQuote(loadData.getEnclose().charAt(0));
        }
        settings.getFormat().setNormalizedNewline(loadData.getLineTerminatedBy().charAt(0));

        /*
         *  fix #1074 : LOAD DATA local INFILE导入的所有Boolean类型全部变成了false
         *  不可见字符将在CsvParser被当成whitespace过滤掉, 使用settings.trimValues(false)来避免被过滤掉
         *  FIXME : 设置trimValues(false)之后, 会引起字段值前后的空白字符无法被过滤!
         */
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
            while ((row = parser.parseNext()) != null) {
                if (ignoreNumber == 0) {
                    parseOneLine(columns, tableName, row, true, loadData.getLineTerminatedBy());
                } else {
                    ignoreNumber--;
                }
            }
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
        tempByteBuffrSize = 0;
        tableName = null;
        partitionColumnIndex = -1;
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
        if (tableConfig.getDirectRouteTC() != null) {
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
