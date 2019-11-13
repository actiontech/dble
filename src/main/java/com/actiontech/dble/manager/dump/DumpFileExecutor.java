package com.actiontech.dble.manager.dump;

import com.actiontech.dble.manager.dump.handler.StatementHandler;
import com.actiontech.dble.manager.dump.handler.StatementHandlerManager;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.parser.ServerParse;
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

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");

    // receive statement in dump file
    private BlockingQueue<String> queue;
    private DumpFileContext context;

    public DumpFileExecutor(BlockingQueue<String> queue, DumpFileWriter writer, DumpFileConfig config) {
        this.queue = queue;
        this.context = new DumpFileContext(writer, config);
    }

    public void start() {
        new Thread(this, "dump-file-executor").start();
    }

    @Override
    public void run() {
        String stmt;
        DumpFileWriter writer = context.getWriter();
        LOGGER.info("begin to parse statement in dump file.");
        while (true) {
            try {
                if (queue.isEmpty()) {
                    LOGGER.info("dump file reader is too slow, please increase read queue size.");
                }

                stmt = queue.take();
                context.setStmt(stmt);
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
                StatementHandler handler = StatementHandlerManager.getHandler(context, statement);
                if (handler.preHandle(context, statement)) {
                    continue;
                }
                handler.handle(context, statement);
            } catch (DumpException | SQLSyntaxErrorException e) {
                String currentStmt = context.getStmt().length() <= 1024 ? context.getStmt() : context.getStmt().substring(0, 1024);
                context.skipCurrentContext();
                LOGGER.warn("current stmt[" + currentStmt + "] error,because:" + e.getMessage());
                context.addError("current stmt[" + currentStmt + "] error,because:" + e.getMessage());
            } catch (Exception e) {
                LOGGER.warn("dump file executor exit, due to :" + e.getMessage());
                try {
                    writer.writeAll(DumpFileReader.EOF);
                } catch (InterruptedException ex) {
                    // ignore
                    LOGGER.warn("dump file executor is interrupted.");
                }
                return;
            }
        }
    }

    public DumpFileContext getContext() {
        return context;
    }

    private boolean preHandle(DumpFileWriter writer, int type, String stmt) throws InterruptedException {
        // push down statement util containing schema
        if (!(ServerParse.CREATE_DATABASE == type || ServerParse.USE == (0xff & type)) && context.getSchema() == null) {
            writer.writeAll(stmt);
            return true;
        }
        // skip view
        if ((ServerParse.MYSQL_CMD_COMMENT == type || ServerParse.MYSQL_COMMENT == type) && skipView(stmt)) {
            return true;
        }
        // footer
        if (stmt.contains("=@OLD_")) {
            writer.writeAll(stmt);
            return true;
        }
        return false;
    }

    private boolean skipView(String stmt) {
        Matcher matcher = DumpFileReader.HINT.matcher(stmt);
        if (matcher.find()) {
            int type = ServerParse.parse(matcher.group(1));
            return type >= ServerParse.CREATE_VIEW && type <= ServerParse.ALTER_VIEW;
        }
        return false;
    }

}
