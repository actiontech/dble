package io.mycat.log.transaction;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.ISO8601DateFormat;

import io.mycat.MycatServer;
import io.mycat.buffer.BufferPool;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.log.DailyRotateLogStore;
import io.mycat.server.ServerConnection;
import io.mycat.util.TimeUtil;

public class TxnLogProcessor extends Thread {
	private static final Logger logger = Logger.getLogger(TxnLogProcessor.class);
	private final DateFormat dateFormat;
	private BlockingQueue<TxnBinaryLog> queue;
	private DailyRotateLogStore store; 

	public TxnLogProcessor(BufferPool bufferPool) {
		this.dateFormat = new ISO8601DateFormat();
		this.queue = new LinkedBlockingQueue<TxnBinaryLog>(256);
		MycatConfig config = MycatServer.getInstance().getConfig();
        SystemConfig systemConfig = config.getSystem();
		this.store = new DailyRotateLogStore(systemConfig.getTransactionLogBaseDir(), systemConfig.getTransactionLogBaseName(),"log",systemConfig.getTransactionRatateSize());
	}

	@Override
	public void run() {
		TxnBinaryLog log = null;
		long flushTime = TimeUtil.currentTimeMillis();
		try {
			store.open();
			for (;;) {
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
				}
				writeLog(log);
			}
		} catch (IOException e) {
			logger.warn("transaction log error:", e);
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

	public void putTxnLog(ServerConnection c, String sql) {
		TxnBinaryLog log = new TxnBinaryLog();
		log.setUser(c.getUser());
		log.setHost(c.getHost());
		log.setSchema(c.getSchema());
		Date date = new Date();
		date.setTime(System.currentTimeMillis());
		log.setExecuteTime(dateFormat.format(date));
		log.setConnid(c.getId()); 
		if (c.isTxstart() || !c.isAutocommit()) { 
			log.setXid(c.getXid());
		} else {
			log.setXid(c.getAndIncrementXid());
		}
		log.setQuery(sql);
		try {
			this.queue.put(log);
		} catch (InterruptedException e) {
			// ignore
		}
	}
}
