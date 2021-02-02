package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.services.manager.dump.handler.StatementHandler;
import com.actiontech.dble.services.manager.dump.handler.StatementHandlerManager;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

/**
 * @author Baofengqi
 */
public final class DumpFileExecutor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    private final BlockingQueue<String> ddlQueue;
    private final BlockingQueue<String> insertQueue;
    private final DumpFileContext context;
    private final NameableExecutor nameableExecutor;
    private final AtomicInteger threadNum = new AtomicInteger(0);

    public DumpFileExecutor(BlockingQueue<String> queue, BlockingQueue<String> insertQueue, DumpFileWriter writer, DumpFileConfig config, SchemaConfig schemaConfig, NameableExecutor nameableExecutor) {
        this.ddlQueue = queue;
        this.insertQueue = insertQueue;
        this.context = new DumpFileContext(writer, config);
        if (schemaConfig != null) {
            this.context.setDefaultSchema(schemaConfig);
        }
        this.nameableExecutor = nameableExecutor;
    }

    @Override
    public void run() {
        this.threadNum.incrementAndGet();
        String stmt = null;
        DumpFileWriter writer = this.context.getWriter();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                //thread pool shrink
                boolean shrink = shrinkThreadPool();
                if (shrink) {
                    break;
                }
                //thread pool expansion
                expansionThreadPool();
                stmt = this.insertQueue.poll();
                if (StringUtil.isBlank(stmt)) {
                    stmt = this.ddlQueue.poll();
                }
                if (StringUtil.isBlank(stmt)) {
                    continue;
                }
                int type = ServerParseFactory.getShardingParser().parse(stmt);
                // pre handle
                if (preHandle(writer, type, stmt)) {
                    continue;
                }
                // finish
                if (stmt.equals(DumpFileReader.EOF)) {
                    writer.writeAll(stmt);
                    LOGGER.info("finish to parse statement in dump file.");
                    break;
                }

                StatementHandler handler = StatementHandlerManager.getHandler(type);
                SQLStatement statement = handler.preHandle(this.context, stmt);
                if (statement == null) {
                    if (!this.context.isSkipContext() || type == ServerParse.UNLOCK) {
                        handler.handle(this.context, stmt);
                    }
                } else {
                    handler.handle(this.context, statement);
                }
            } catch (DumpException | SQLSyntaxErrorException e) {
                assert stmt != null;
                String currentStmt = stmt.length() <= 1024 ? stmt : stmt.substring(0, 1024);
                this.context.setSkipContext(true);
                LOGGER.warn("current stmt[" + currentStmt + "] error.", e);
                this.context.addError("current stmt[" + currentStmt + "] error,because:" + e.getMessage());
            } catch (InterruptedException ie) {
                LOGGER.warn("dump file executor is interrupted.");
                return;
            } catch (Exception e) {
                LOGGER.warn("dump file executor exit", e);
                this.context.addError("dump file executor exit, because:" + e.getMessage());
                stopWriter(writer);
                this.nameableExecutor.shutdown();
                break;
            }
        }
    }

    private void expansionThreadPool() {
        if (!this.insertQueue.isEmpty() && this.threadNum.get() == 1) {
            for (int i = 0; i < this.nameableExecutor.getCorePoolSize() - 1; i++) {
                this.nameableExecutor.execute(this);
            }
        }
    }

    private boolean shrinkThreadPool() {
        if (!this.ddlQueue.isEmpty() && this.threadNum.get() > 1) {
            if (this.threadNum.decrementAndGet() != 0) {
                return true;
            } else {
                //keep the last thread
                this.threadNum.incrementAndGet();
            }
        }
        //shrink time point-ddlQueue/insertQueue both empty
        if (this.insertQueue.isEmpty() && this.threadNum.get() > 1) {
            if (this.threadNum.decrementAndGet() != 0) {
                return true;
            } else {
                //keep the last thread
                this.threadNum.incrementAndGet();
            }
        }
        return false;
    }

    public DumpFileContext getContext() {
        return this.context;
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

    private boolean preHandle(DumpFileWriter writer, int type, String stmt) throws
            InterruptedException, RuntimeException {
        // push down statement util containing sharding
        if (!(ServerParse.CREATE_DATABASE == type || ServerParse.USE == (0xff & type)) && this.context.getSchema() == null) {
            if (ServerParse.DDL == type || ServerParse.INSERT == type || ServerParse.LOCK == type) {
                throw new RuntimeException("Please set schema by -s option or make sure that there are statement about schema in dump file.");
            }
            writer.writeAll(stmt);
            return true;
        }
        // skip view
        if (ServerParse.MYSQL_CMD_COMMENT == type || ServerParse.MYSQL_COMMENT == type) {
            return skipView(stmt);
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
            this.context.addError("skip view " + matcher.group(1));
            return true;
        }
        matcher = DumpFileReader.CREATE_VIEW1.matcher(stmt);
        return matcher.find();
    }

}
