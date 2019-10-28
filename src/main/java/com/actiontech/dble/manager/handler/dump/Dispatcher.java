/*
 * Copyright (C) 2016-2019 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.handler.dump;

import com.actiontech.dble.manager.handler.dump.type.DumpContent;
import com.actiontech.dble.manager.handler.dump.type.DumpSchema;
import com.actiontech.dble.manager.handler.dump.type.DumpTable;
import com.actiontech.dble.manager.handler.dump.type.handler.DumpTableHandlerManager;
import com.actiontech.dble.manager.handler.dump.type.handler.DumpHandler;
import com.actiontech.dble.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Baofengqi
 */
public final class Dispatcher implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);

    public static final Pattern DATABASE = Pattern.compile(".*--\\s+Current\\s+Database:\\s+`([a-zA-z0-9]+)`.*", Pattern.CASE_INSENSITIVE);
    public static final Pattern TABLE = Pattern.compile(".*--\\s+Table\\s+structure\\s+for\\s+table\\s+`([a-zA-z0-9]+)`.*", Pattern.CASE_INSENSITIVE);

    private DumpContent dumpContent = null;

    // receive statement in dump file
    private BlockingQueue<String> queue;
    // process table
    private ExecutorService executorService = ExecutorUtil.createFixed("dump", 5);
    private DumpFileWriter writer;

    public Dispatcher(BlockingQueue<String> queue, DumpFileWriter writer) {
        this.queue = queue;
        this.writer = writer;
    }

    @Override
    public void run() {
        String currentSchema = null;
        boolean inHeader = true;
        boolean inFooter = false;
        while (true) {
            String stmt;
            try {
                stmt = queue.take();
            } catch (InterruptedException e) {
                LOGGER.warn("Dispather thread is interrupted, exit.");
                return;
            }
            // write header
            if (inHeader && !beginContent(stmt)) {
                if (dumpContent == null) {
                    dumpContent = new DumpContent();
                }
                dumpContent.add(stmt);
                continue;
            }
            inHeader = false;

            // write footer
            if (inFooter) {
                dumpContent.add(stmt);
                if (stmt.contains("-- Dump completed")) {
                    dumpContent.setFooter(true);
                    execute();
                    executorService.shutdown();
                    return;
                }
                continue;
            }

            // parse schema
            if (stmt.contains("-- Current Database")) {
                Matcher m = DATABASE.matcher(stmt);
                if (m.find()) {
                    String schema = m.group(1);
                    execute();
                    dumpContent = new DumpSchema(schema);
                    currentSchema = schema;
                }
            } else if (stmt.contains("-- Table structure for table")) {
                Matcher m = TABLE.matcher(stmt);
                if (m.find()) {
                    String tableName = m.group(1);
                    execute();
                    dumpContent = new DumpTable(currentSchema, tableName);
                }
            } else if (stmt.contains("OLD_")) {
                // footer
                inFooter = true;
                execute();
                dumpContent = new DumpContent();
            }
            dumpContent.add(stmt);
        }
    }

    private boolean beginContent(String stmt) {
        return stmt.contains("-- Current Database") || stmt.contains("-- Table structure");
    }

    private void execute() {
        if (dumpContent != null) {
            ProcessTableTask task = new ProcessTableTask(dumpContent);
            executorService.execute(task);
            writer.write(dumpContent);
            dumpContent = null;
        }
    }

    class ProcessTableTask implements Runnable {

        private DumpContent dumpContent;

        ProcessTableTask(DumpContent dumpContent) {
            this.dumpContent = dumpContent;
        }

        @Override
        public void run() {
            DumpHandler handler = DumpTableHandlerManager.getHandler(dumpContent);
            if (handler == null) {
                dumpContent.setCanDump(true);
                return;
            }
            try {
                handler.handle(dumpContent);
            } catch (SQLSyntaxErrorException e) {
                writer.writeError(dumpContent.toString() + " skip, because:" + e.getMessage());
            }
            dumpContent.setCanDump(true);
        }
    }
}
