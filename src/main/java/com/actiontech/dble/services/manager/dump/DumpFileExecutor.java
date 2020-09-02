package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.manager.dump.handler.StatementHandler;
import com.actiontech.dble.services.manager.dump.handler.StatementHandlerManager;
import com.actiontech.dble.util.TimeUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;

/**
 * @author Baofengqi
 */
public final class DumpFileExecutor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private BlockingQueue<String> queue;
    private DumpFileContext context;
    private volatile boolean isStop = false;

    public DumpFileExecutor(BlockingQueue<String> queue, DumpFileWriter writer, DumpFileConfig config, SchemaConfig schemaConfig) {
        this.queue = queue;
        this.context = new DumpFileContext(writer, config);
        if (schemaConfig != null) {
            this.context.setDefaultSchema(schemaConfig);
        }
    }

    public void start() {
        Thread self = new Thread(this, "dump-file-executor");
        self.start();
    }

    public void stop() {
        LOGGER.info("finish to parse statement in dump file, because executor is stopped.");
        isStop = true;
        stopWriter(context.getWriter());
    }

    public boolean isStop() {
        return isStop;
    }

    @Override
    public void run() {
        String stmt = null;
        DumpFileWriter writer = context.getWriter();
        long startTime = TimeUtil.currentTimeMillis();
        LOGGER.info("begin to parse statement in dump file.");
        while (!isStop) {
            try {
                stmt = queue.take();
                if (LOGGER.isDebugEnabled()) {
                    long endTime = TimeUtil.currentTimeMillis();
                    if (endTime - startTime > 1000) {
                        startTime = endTime;
                        if (queue.isEmpty()) {
                            LOGGER.debug("dump file reader is slow, you can try increasing read queue size.");
                        }
                    }
                }

                int type = ServerParse.parse(stmt);
                // pre handle
                if (preHandle(writer, type, stmt)) {
                    continue;
                }
                // finish
                if (stmt.equals(DumpFileReader.EOF)) {
                    writer.writeAll(stmt);
                    LOGGER.info("finish to parse statement in dump file.");
                    return;
                }

                StatementHandler handler = StatementHandlerManager.getHandler(type);
                SQLStatement statement = handler.preHandle(context, stmt);
                if (statement == null) {
                    if (!context.isSkipContext() || type == ServerParse.UNLOCK) {
                        handler.handle(context, stmt);
                    }
                } else {
                    handler.handle(context, statement);
                }

            } catch (DumpException | SQLSyntaxErrorException e) {
                assert stmt != null;
                String currentStmt = stmt.length() <= 1024 ? stmt : stmt.substring(0, 1024);
                context.setSkipContext(true);
                LOGGER.warn("current stmt[" + currentStmt + "] error.", e);
                context.addError("current stmt[" + currentStmt + "] error,because:" + e.getMessage());
            } catch (InterruptedException ie) {
                LOGGER.warn("dump file executor is interrupted.");
                return;
            } catch (Exception e) {
                LOGGER.warn("dump file executor exit", e);
                context.addError("dump file executor exit, because:" + e.getMessage());
                stopWriter(writer);
                isStop = true;
            }
        }
    }

    public DumpFileContext getContext() {
        return context;
    }

    private void stopWriter(DumpFileWriter writer) {
        try {
            writer.setDeleteFile(true);
            writer.writeAll(DumpFileReader.EOF);
        } catch (InterruptedException ex) {
            // ignore
            LOGGER.warn("dump file executor is interrupted.");
        }
    }

    private boolean preHandle(DumpFileWriter writer, int type, String stmt) throws InterruptedException, RuntimeException {
        // push down statement util containing sharding
        if (!(ServerParse.CREATE_DATABASE == type || ServerParse.USE == (0xff & type)) && context.getSchema() == null) {
            if (ServerParse.DDL == type || ServerParse.INSERT == type || ServerParse.LOCK == type) {
                throw new RuntimeException("Please set schema by -s option or make sure that there are statement about schema in dump file.");
            }
            writer.writeAll(stmt);
            return true;
        }
        // skip view
        if ((ServerParse.MYSQL_CMD_COMMENT == type || ServerParse.MYSQL_COMMENT == type) && skipView(stmt)) {
            context.setSkipContext(true);
            return true;
        }
        // footer
        if (stmt.contains("=@OLD_")) {
            writer.writeAll(stmt);
            return true;
        }
        return stmt.contains("Dump completed");
    }

    private boolean skipView(String stmt) {
        Matcher matcher = DumpFileReader.CREATE_VIEW.matcher(stmt);
        if (matcher.find()) {
            context.addError("skip view " + matcher.group(1));
        }
        return false;
    }

}
