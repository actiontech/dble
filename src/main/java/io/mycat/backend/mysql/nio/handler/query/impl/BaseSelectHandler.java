package io.mycat.backend.mysql.nio.handler.query.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.config.MycatConfig;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

/**
 * 仅用来执行Sql，将接收到的数据转发到下一个handler
 * 
 */
public class BaseSelectHandler extends BaseDMLHandler {
	private static final Logger logger = Logger.getLogger(BaseSelectHandler.class);

	private final boolean autocommit;
	private volatile int fieldCounts = -1;

	private RouteResultsetNode rrss;

	/**
	 * @param route
	 * @param autocommit
	 * @param orderBys
	 * @param session
	 */
	public BaseSelectHandler(long id, RouteResultsetNode rrss, boolean autocommit, NonBlockingSession session) {
		super(id, session);
		this.rrss = rrss;
		this.autocommit = autocommit;
	}

	public MySQLConnection initConnection() throws Exception {
		if (session.closed()) {
			return null;
		}

		MySQLConnection exeConn = (MySQLConnection) session.getTarget(rrss);
		if (session.tryExistsCon(exeConn, rrss)) {
			return exeConn;
		} else {
			MycatConfig conf = MycatServer.getInstance().getConfig();
			PhysicalDBNode dn = conf.getDataNodes().get(rrss.getName());
			final BackendConnection newConn=  dn.getConnection(dn.getDatabase(), session.getSource().isAutocommit());
			session.bindConnection(rrss, newConn);
			return (MySQLConnection)newConn;
		}
	}

	public void execute(MySQLConnection conn) {
		if (session.closed()) {
			session.clearResources(true);
			return;
		}
		conn.setResponseHandler(this);
		if (logger.isInfoEnabled()) {
			logger.info(conn.toString() + " send sql:" + rrss.getStatement());
		}
		if (session.closed()) {
			session.onQueryError("failed or cancelled by other thread".getBytes());
			return;
		}
		conn.execute(rrss, session.getSource(), autocommit);
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		conn.syncAndExcute();
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
			boolean isLeft, BackendConnection conn) {
		if (logger.isDebugEnabled()) {
			logger.debug(conn.toString() + "'s field is reached.");
		}
		if (terminate.get()) {
			return;
		}
		if (fieldCounts == -1) {
			fieldCounts = fields.size();
		}
		List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();

		for (int i = 0; i < fields.size(); i++) {
			FieldPacket field = new FieldPacket();
			field.read(fields.get(i));
			fieldPackets.add(field);
		}
		nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
	}

	@Override
	public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
		if (terminate.get())
			return true;
		RowDataPacket rp = new RowDataPacket(fieldCounts);
		rp.read(row);
		nextHandler.rowResponse(null, rp, this.isLeft, conn);
		return false;
	}

	@Override
	public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
		if (logger.isDebugEnabled()) {
			logger.debug(conn.toString() + " 's rowEof is reached.");
		}
		((MySQLConnection)conn).setRunning(false);
		if (this.terminate.get())
			return;
		nextHandler.rowEofResponse(data, this.isLeft, conn);
	}

	/**
	 * 1、if some connection's thread status is await. 2、if some connection's
	 * thread status is running.
	 */
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		if (terminate.get())
			return;
		logger.warn(
				new StringBuilder().append(conn.toString()).append("|connectionError()|").append(e.getMessage()).toString());
		session.onQueryError(e.getMessage().getBytes());
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		((MySQLConnection)conn).setRunning(false);
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(err);
		String errMsg;
		try {
			errMsg = new String(errPacket.message,conn.getCharset());
		} catch (UnsupportedEncodingException e) {
			errMsg ="UnsupportedEncodingException:"+conn.getCharset();
		}
		logger.warn(conn.toString() + errMsg);
		if (terminate.get())
			return;
		session.onQueryError(errMsg.getBytes());
	}

	@Override
	protected void onTerminate() {
		this.session.releaseConnection(rrss, logger.isDebugEnabled(), false);
	}

	@Override
	public HandlerType type() {
		return HandlerType.BASESEL;
	}

}
