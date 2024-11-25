/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.split.loaddata;

import com.oceanbase.obsharding_d.backend.mysql.store.fs.FileUtils;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;
import com.oceanbase.obsharding_d.services.manager.dump.ErrorMsg;
import com.oceanbase.obsharding_d.services.manager.handler.SplitLoadDataHandler;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class DumpFileReader implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";

    private final List<ErrorMsg> errorMsgList;
    private final AtomicBoolean errorFlag;
    private final AtomicInteger nodeCount;
    private String filename;
    private final SplitLoadDataHandler.Config config;
    private final Map<String, ShardingNodeWriter> writerMap;
    private CsvParser parser;
    private AbstractPartitionAlgorithm algorithm;
    private List<String> shardingNodes;

    public DumpFileReader(List<ErrorMsg> errorMsgList, AtomicBoolean errorFlag, SplitLoadDataHandler.Config config, Map<String, ShardingNodeWriter> writerMap, AtomicInteger nodeCount) {
        this.errorMsgList = errorMsgList;
        this.errorFlag = errorFlag;
        this.config = config;
        this.writerMap = writerMap;
        this.nodeCount = nodeCount;
    }

    public void open(String fileName) {
        this.filename = fileName;
        this.algorithm = ((ShardingTableConfig) config.getTableConfig()).getFunction();
        this.shardingNodes = config.getTableConfig().getShardingNodes();

        CsvParserSettings settings = new CsvParserSettings();
        settings.setMaxColumns(1017);
        settings.setMaxCharsPerColumn(65535);
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(",");
        settings.getFormat().setComment('\0');
        settings.getFormat().setQuote('\0');
        settings.getFormat().setQuoteEscape('\\');
        settings.getFormat().setNormalizedNewline('\n');
        settings.setSkipEmptyLines(false);
        settings.trimValues(false);
        settings.setEmptyValue("");

        this.parser = new CsvParser(settings);
    }

    @Override
    public void run() {
        try {
            parser.beginParsing(new File(filename));
            String[] row;
            while ((row = parser.parseNext()) != null) {
                if ((row.length == 1 && row[0] == null) || (row.length == 1 && row[0].isEmpty()) || row.length == 0) {
                    continue;
                }
                String shardingColumnVal = row[0];
                Integer nodeIndex = algorithm.calculate(shardingColumnVal);
                String shardingNode = shardingNodes.get(nodeIndex);

                ShardingNodeWriter initialization = new ShardingNodeWriter();
                ShardingNodeWriter shardingNodeWriter = writerMap.computeIfAbsent(shardingNode, key -> initialization);
                if (initialization == shardingNodeWriter) {
                    //init writer
                    String sourceFilename = FileUtils.getName(config.getSourcePath());
                    String fileName = String.format("%s-%s-%d.%s", config.getTargetPath() + sourceFilename, shardingNode, new Date().getTime(), FileUtils.getExtension(sourceFilename));
                    shardingNodeWriter.open(fileName, shardingNode, nodeCount, errorMsgList, errorFlag, config);
                }
                shardingNodeWriter.write(row, errorFlag);
            }
            //eof
            for (Map.Entry<String, ShardingNodeWriter> nodeWriterEntry : writerMap.entrySet()) {
                nodeWriterEntry.getValue().write(new String[]{EOF}, errorFlag);
            }
        } catch (Exception | Error e) {
            LOGGER.warn("split reader error,", e);
            errorFlag.compareAndSet(false, true);
            errorMsgList.add(new ErrorMsg("split loaddata unknown error[exit]", e.getMessage()));
        } finally {
            parser.stopParsing();
        }
    }
}

