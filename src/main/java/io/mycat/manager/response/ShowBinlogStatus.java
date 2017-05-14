package io.mycat.manager.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.StringUtil;

public class ShowBinlogStatus {
	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final String[] FIELDS = new String[]{"File","Position","Binlog_Do_DB","Binlog_Ignore_DB","Executed_Gtid_Set"};
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Url", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		for (int j = 0; j < FIELDS.length; j++) {
			fields[i] = PacketUtil.getField(FIELDS[j], Fields.FIELD_TYPE_VAR_STRING);
			fields[i++].packetId = ++packetId;
		}
		eof.packetId = ++packetId;
	}
	private static final String SHOW_BINLOG_QUERY ="SHOW MASTER STATUS";
	private static Logger logger = LoggerFactory.getLogger(ShowBinlogStatus.class);
	private static AtomicInteger sourceCount;
	private static List<RowDataPacket> rows;
	private static String errMsg = null;
	public static void execute(ManagerConnection c) {
		if (MycatServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
			errMsg = null;
			waitAllSession();
			getQueryResult(c.getCharset());
			while(sourceCount.get()>0){
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
			}
			if(errMsg ==null){
				ByteBuffer buffer = c.allocate();
				buffer = header.write(buffer, c, true);
				for (FieldPacket field : fields) {
					buffer = field.write(buffer, c, true);
				}
				buffer = eof.write(buffer, c, true);
				byte packetId = eof.packetId;
				for (RowDataPacket row : rows) {
					row.packetId = ++packetId;
					buffer = row.write(buffer, c, true);
				}
				rows.clear();
				EOFPacket lastEof = new EOFPacket();
				lastEof.packetId = ++packetId;
				buffer = lastEof.write(buffer, c, true);
				c.write(buffer);
			} else{
				c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errMsg);
				errMsg =null;
			}
			MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
		} else {
			c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
		}
	}

	private static void waitAllSession() {
		logger.info("waiting all transaction sessions which are not finished.");

		List<NonBlockingSession> fcList = new ArrayList<NonBlockingSession>();
		for (NIOProcessor process : MycatServer.getInstance().getProcessors()) {
			for (FrontendConnection front : process.getFrontends().values()) {
				if (!(front instanceof ServerConnection)) {
					continue;
				}
				ServerConnection sc = (ServerConnection) front;
				NonBlockingSession session = sc.getSession2();
				if (session.isNeedWaitFinished()) {
					fcList.add(session);
				}
			}
		}
		while (!fcList.isEmpty()) {
			Iterator<NonBlockingSession> sListIterator = fcList.iterator();
			while (sListIterator.hasNext()) {
				NonBlockingSession session = sListIterator.next();
				if (!session.isNeedWaitFinished()) {
					sListIterator.remove();
				}
			}
		}
		logger.info("all transaction session are paused.");
	}
	private static void getQueryResult(final String charset){
		Collection<PhysicalDBPool> allPools = MycatServer.getInstance().getConfig().getDataHosts().values();
		sourceCount = new AtomicInteger(allPools.size());
		rows = new ArrayList<RowDataPacket>(allPools.size());
		for(PhysicalDBPool pool:allPools){
			//if WRITE_RANDOM_NODE ,may the binlog is not ready.
			final PhysicalDatasource source = pool.getSource();
			OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(FIELDS,
					new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
						@Override
						public void onResult(SQLQueryResult<Map<String, String>> result) {
							String url = source.getConfig().getUrl();
							if (!result.isSuccess()) {
								errMsg = "Getting binlog status from this instance["+url+"] is failed";
							} else {
								rows.add(getRow(url, result.getResult(), charset));
							}
							sourceCount.decrementAndGet();
						}

					});
			SQLJob sqlJob = new SQLJob(SHOW_BINLOG_QUERY, pool.getSchemas()[0], resultHandler, source);
			sqlJob.run();
		}
	}

	private static RowDataPacket getRow(String url, Map<String, String> result, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(url, charset));
		for (int j = 0; j < FIELDS.length; j++) {
			row.add(StringUtil.encode(result.get(FIELDS[j]), charset));
		}
		return row;
	}
}
