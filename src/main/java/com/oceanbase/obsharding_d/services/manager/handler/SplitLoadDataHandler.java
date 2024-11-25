/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.dump.ErrorMsg;
import com.oceanbase.obsharding_d.services.manager.response.DumpFileError;
import com.oceanbase.obsharding_d.services.manager.split.loaddata.DumpFileReader;
import com.oceanbase.obsharding_d.services.manager.split.loaddata.ShardingNodeWriter;
import com.oceanbase.obsharding_d.util.ExecutorUtil;
import com.oceanbase.obsharding_d.util.NameableExecutor;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SplitLoadDataHandler {
    private static final Pattern SPLIT_STMT = Pattern.compile("([^\\s]+)\\s+([^\\s]+)(((\\s*(-s([^\\s]+))?)|(\\s+(-t([^\\s]+))?)|(\\s+(-ds(\\d+))?))+)", Pattern.CASE_INSENSITIVE);
    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");

    private SplitLoadDataHandler() {
    }


    public static void handle(String stmt, ManagerService service, int offset) {
        Config config = parseOption(stmt.substring(offset).trim());
        if (config == null) {
            LOGGER.info("split load data syntax is error.");
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax");
            return;
        }
        if (StringUtil.isBlank(config.getSourcePath()) || StringUtil.isBlank(config.getTargetPath()) || StringUtil.isBlank(config.getSchemaName()) || StringUtil.isBlank(config.getTableName())) {
            LOGGER.info("split load data syntax is error.");
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "You have an error in your SQL syntax");
            return;
        }
        BaseTableConfig tableConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(config.getSchemaName()).getTables().get(config.getTableName());
        if (null == tableConfig) {
            LOGGER.info("schema `{}` table `{}` configuration does not exist.", config.getSchemaName(), config.getTableName());
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "schema `{" + config.getSchemaName() + "}` table `{" + config.getTableName() + "}` configuration does not exist.");
            return;
        }
        config.setTableConfig(tableConfig);

        List<ErrorMsg> errorMsgList = Lists.newCopyOnWriteArrayList();
        AtomicBoolean errorFlag = new AtomicBoolean();
        AtomicInteger nodeCount = new AtomicInteger(-1);

        Map<String, ShardingNodeWriter> writerMap = new ConcurrentHashMap<>();
        NameableExecutor fileReadExecutor = ExecutorUtil.createFixed("splitLoadReader", 1);
        DumpFileReader reader = new DumpFileReader(errorMsgList, errorFlag, config, writerMap, nodeCount);

        // start read
        reader.open(config.getSourcePath());
        fileReadExecutor.execute(reader);


        //wait
        while (!errorFlag.get() && nodeCount.get() != 0) {
            LockSupport.parkNanos(1000);
        }

        //recycle thread
        fileReadExecutor.shutdownNow();
        for (ShardingNodeWriter shardingNodeWriter : writerMap.values()) {
            shardingNodeWriter.shutdown();
        }
        writerMap.clear();

        //handle error
        if (errorFlag.get()) {
            //todo whether the generated files are deleted
            DumpFileError.execute(service, errorMsgList);
            return;
        }

        OkPacket packet = new OkPacket();
        packet.setServerStatus(2);
        packet.setPacketId(1);
        packet.write(service.getConnection());
    }


    private static Config parseOption(String stmt) {
        Matcher m = SPLIT_STMT.matcher(stmt);
        if (m.matches()) {
            Config config = new Config();
            config.setSourcePath(m.group(1));
            config.setTargetPath(m.group(2));
            config.setSchemaName(m.group(7));
            config.setTableName(m.group(10));
            if (null != m.group(13)) {
                config.setDisruptorBufferSize(Integer.parseInt(m.group(13)));
            }
            return config;
        }
        return null;
    }

    public static class Config {
        private String sourcePath;
        private String targetPath;
        private String schemaName;
        private String tableName;
        private BaseTableConfig tableConfig;
        private int disruptorBufferSize = 512;

        public Config() {
        }

        public void setSourcePath(String sourcePath) {
            this.sourcePath = sourcePath;
        }

        public void setTargetPath(String targetPath) {
            this.targetPath = targetPath;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setTableConfig(BaseTableConfig tableConfig) {
            this.tableConfig = tableConfig;
        }

        public int getDisruptorBufferSize() {
            return disruptorBufferSize;
        }

        public void setDisruptorBufferSize(int disruptorBufferSize) {
            this.disruptorBufferSize = disruptorBufferSize;
        }

        public String getSourcePath() {
            return sourcePath;
        }

        public String getTargetPath() {
            return targetPath;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public BaseTableConfig getTableConfig() {
            return tableConfig;
        }
    }

}
