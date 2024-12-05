/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump;

import com.oceanbase.obsharding_d.btrace.provider.SplitFileProvider;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.services.manager.dump.handler.StatementHandler;
import com.oceanbase.obsharding_d.services.manager.dump.handler.StatementHandlerManager;
import com.oceanbase.obsharding_d.util.NameableExecutor;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean errorFlag;

    public DumpFileExecutor(BlockingQueue<String> queue, BlockingQueue<String> insertQueue, DumpFileWriter writer, DumpFileConfig config,
                            SchemaConfig schemaConfig, NameableExecutor nameableExecutor, AtomicBoolean flag) {
        this.ddlQueue = queue;
        this.insertQueue = insertQueue;
        this.context = new DumpFileContext(writer, config);
        if (schemaConfig != null) {
            this.context.setDefaultSchema(schemaConfig);
        }
        this.nameableExecutor = nameableExecutor;
        this.errorFlag = flag;
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
                if (StringUtil.isEmpty(stmt)) {
                    stmt = this.ddlQueue.poll();
                }
                SplitFileProvider.getReadQueueSizeOfPoll(this.insertQueue.size());
                if (StringUtil.isEmpty(stmt)) {
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
            } catch (DumpException e) {
                assert stmt != null;
                String currentStmt = stmt.length() <= 1024 ? stmt : stmt.substring(0, 1024);
                this.context.setSkipContext(true);
                LOGGER.warn("current stmt[" + currentStmt + "] error.", e);
                this.context.addError("current stmt[" + currentStmt + "] error,because:" + e.getMessage());
            } catch (InterruptedException e) {
                LOGGER.debug("dump file executor is interrupted.");
                break;
            } catch (Exception | Error e) {
                LOGGER.warn("dump file executor exit", e);
                this.context.addError("dump file executor exit, because:" + e.getMessage());
                errorFlag.compareAndSet(false, true);
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

    private boolean preHandle(DumpFileWriter writer, int type, String stmt) throws
            RuntimeException {
        // push down statement util containing sharding
        if (!(ServerParse.CREATE_DATABASE == type || ServerParse.USE == (0xff & type)) && this.context.getSchema() == null) {
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
            this.context.addError("skip view " + matcher.group(1));
            return true;
        }
        matcher = DumpFileReader.CREATE_VIEW1.matcher(stmt);
        return matcher.find();
    }

}
