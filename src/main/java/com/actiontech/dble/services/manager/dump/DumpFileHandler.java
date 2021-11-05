package com.actiontech.dble.services.manager.dump;


import com.actiontech.dble.btrace.provider.SplitFileProvider;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Baofengqi
 */
public final class DumpFileHandler implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");

    private BlockingQueue<String> handleQueue;
    private final BlockingQueue<String> ddlQueue;
    private final BlockingQueue<String> insertQueue;

    private StringBuilder tempStr = new StringBuilder(1000);
    private NameableExecutor nameableExecutor;
    ManagerService service;


    public DumpFileHandler(ArrayBlockingQueue<String> queue, ArrayBlockingQueue<String> insertDeque, BlockingQueue<String> handleQueue, NameableExecutor nameableExecutor, ManagerService service) {
        this.ddlQueue = queue;
        this.insertQueue = insertDeque;
        this.handleQueue = handleQueue;
        this.nameableExecutor = nameableExecutor;
        this.service = service;
    }

    @Override
    public void run() {

        while (true) {
            try {
                String stmts = handleQueue.take();
                SplitFileProvider.getHandleQueueSizeOfTake(handleQueue.size());
                if (stmts == null || stmts.isEmpty()) {
                    continue;
                }
                if (stmts.equals("dump file eof")) {
                    if (null != tempStr && !StringUtil.isBlank(tempStr.toString())) {
                        putSql(tempStr.toString());
                        this.tempStr = null;
                    }
                    putSql("dump file eof");
                    break;
                }
                readSQLByEOF(stmts);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    // read one statement by ;
    private void readSQLByEOF(String stmts) throws InterruptedException {
        boolean endWithEOF = stmts.endsWith(";") | stmts.endsWith(";\n");
        List<String> strings = DumpFileReader.splitContent(stmts, ";\n");
        int len = strings.size() - 1;

        int i = 0;
        if (len > 0 && tempStr != null && !StringUtil.isEmpty(tempStr.toString())) {
            tempStr.append(strings.get(0));
            putSql(tempStr.toString());
            tempStr = null;
            i = 1;
        }

        for (; i < len; i++) {
            if (!StringUtil.isEmpty(strings.get(i))) {
                putSql(strings.get(i));
            }
        }


        if (!endWithEOF) {
            if (tempStr == null) {
                tempStr = new StringBuilder(strings.get(len));
            } else {
                tempStr.append(strings.get(len));
            }
        } else {
            if (tempStr != null && !StringUtil.isEmpty(tempStr.toString())) {
                tempStr.append(strings.get(len));
                putSql(tempStr.toString());
                tempStr = null;
            } else {
                if (!StringUtil.isEmpty(strings.get(len))) {
                    putSql(strings.get(len));
                }
            }
        }
    }

    public void putSql(String sql) throws InterruptedException {
        if (StringUtil.isEmpty(sql)) {
            return;
        }

        int type = ServerParseFactory.getShardingParser().parse(sql);
        if (ServerParse.INSERT == type) {
            while (!this.ddlQueue.isEmpty() && !this.nameableExecutor.isShutdown()) {
                LockSupport.parkNanos(1000);
            }
            this.insertQueue.put(sql);
            SplitFileProvider.getReadQueueSizeOfPut(this.insertQueue.size());
        } else {
            while (!this.insertQueue.isEmpty() && !this.nameableExecutor.isShutdown()) {
                LockSupport.parkNanos(1000);
            }
            this.ddlQueue.put(sql);
        }
    }
}
