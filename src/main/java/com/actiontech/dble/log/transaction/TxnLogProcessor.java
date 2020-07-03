/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.transaction;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.DailyRotateLogStore;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TxnLogProcessor extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(TxnLogProcessor.class);
    private final DateFormat dateFormat;
    private BlockingQueue<TxnBinaryLog> queue;
    private DailyRotateLogStore store;

    public TxnLogProcessor() {
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        this.queue = new LinkedBlockingQueue<>(256);
        this.store = new DailyRotateLogStore(SystemConfig.getInstance().getTransactionLogBaseDir(), SystemConfig.getInstance().getTransactionLogBaseName(), "log", SystemConfig.getInstance().getTransactionRotateSize(), null);
    }

    @Override
    public void run() {
        TxnBinaryLog log = null;
        long flushTime = TimeUtil.currentTimeMillis();
        try {
            store.open();
            for (; ; ) {
                while ((log = queue.poll()) != null) {
                    writeLog(log);
                }
                long interval = TimeUtil.currentTimeMillis() - flushTime;
                if (interval > 1000) {
                    store.force(false);
                    flushTime = TimeUtil.currentTimeMillis();
                }
                try {
                    log = queue.take();
                } catch (InterruptedException e) {
                    //ignore error
                }
                writeLog(log);
            }
        } catch (IOException e) {
            LOGGER.info("transaction log error:", e);
            store.close();
        }
    }

    private void writeLog(TxnBinaryLog log) throws IOException {
        if (log == null)
            return;
        byte[] data = null;
        try {
            data = log.toString().getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        store.write(buffer);
    }

    public void putTxnLog(ShardingService service, String sql) {
        TxnBinaryLog log = new TxnBinaryLog();
        log.setUser(service.getUser());
        log.setHost(service.getConnection().getHost());
        log.setSchema(service.getSchema());
        Date date = new Date();
        date.setTime(System.currentTimeMillis());
        log.setExecuteTime(dateFormat.format(date));
        log.setConnId(service.getConnection().getId());
        if (service.isTxStart() || !service.isAutocommit()) {
            log.setXid(service.getXid());
        } else {
            log.setXid(service.getAndIncrementXid());
        }
        log.setQuery(sql);
        try {
            this.queue.put(log);
        } catch (InterruptedException e) {
            //ignore error
        }
    }
}
