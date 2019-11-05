package com.actiontech.dble.manager.dump;

import com.actiontech.dble.manager.dump.handler.StatementHandler;
import com.actiontech.dble.manager.dump.handler.StatementHandlerManager;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.SQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.concurrent.BlockingQueue;

/**
 * @author Baofengqi
 */
public final class DumpFileExecutor implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger(DumpFileExecutor.class);

    // receive statement in dump file
    private BlockingQueue<String> queue;
    private DumpFileContext context;

    public DumpFileExecutor(BlockingQueue<String> queue, DumpFileWriter writer) {
        this.queue = queue;
        this.context = new DumpFileContext(writer);
    }

    @Override
    public void run() {
        String stmt;
        DumpFileWriter writer = context.getWriter();
        while (true) {
            try {
                stmt = queue.take();
                context.setStmt(stmt);
                int type = ServerParse.parse(stmt);
                if (ServerParse.CREATE_DATABASE != type && context.getSchema() == null) {
                    writer.writeAll(stmt);
                    continue;
                }
                // footer
                if (stmt.contains("=@OLD_")) {
                    writer.writeAll(stmt);
                    continue;
                }
                if (stmt.equals(DumpFileReader.EOF)) {
                    writer.writeAll(stmt);
                    return;
                }

                SQLStatement statement = null;
                // parse ddl or create database
                if (ServerParse.DDL == type || ServerParse.CREATE_DATABASE == type || ServerParse.USE == (0xff & type)) {
                    stmt = stmt.replace("/*!", "/*#");
                    statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
                }
                // if ddl is wrong，the following statement is skip.
                if (context.isSkip()) {
                    continue;
                }
                if (ServerParse.INSERT == type && !context.isPushDown()) {
                    statement = RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
                }
                StatementHandler handler = StatementHandlerManager.getHandler(statement);
                handler.handle(context, statement);

            } catch (SQLNonTransientException e) {
                context.skipCurrentContext();
                context.addError(e.getMessage());
            } catch (InterruptedException e) {
                try {
                    writer.writeAll(DumpFileReader.EOF);
                } catch (InterruptedException ex) {
                    // ignore
                    LOGGER.warn("dump file executor exit.");
                }
            }
        }
    }

    public DumpFileContext getContext() {
        return context;
    }

}
