package io.mycat.backend.mysql.nio.handler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import io.mycat.backend.BackendConnection;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;


public class TransactionHandler implements ResponseHandler{
//	public static enum TxOperation {
//		BEGIN, COMMIT, SAVEPOINT, RELEASE_SAVEPOINT, ROLLBACK_TO_SAVEPOINT, ROLLBACK
//	}
	public static enum TxOperation {
		 COMMIT, ROLLBACK
	}

	private static Logger logger = Logger.getLogger(TransactionHandler.class);
	private RouteResultsetNode[] nodes;
	private NonBlockingSession session;
	private TxOperation txOperation;
	private AtomicInteger nodeCount;
	private byte[] sendData;
	private volatile boolean isFail;
	private ErrorPacket error;
	private Set<BackendConnection> errConnection;
	private String closeReason;
	public TransactionHandler(RouteResultsetNode[] nodes,Set<BackendConnection> errConnection, NonBlockingSession session, TxOperation txOperation , String closeReason){
		this.nodes = nodes;
		this.errConnection = errConnection;
		this.session = session;
		this.txOperation = txOperation;
		this.closeReason = closeReason;
		this.nodeCount = new AtomicInteger(nodes.length-errConnection.size());
		this.isFail = false;
	}
	
	public void execute(byte[] packet) {
		this.sendData = packet;
		if (nodeCount.get() == 0) {
			session.getSource().write(sendData);
			session.getSource().close(closeReason);
			return;
		}
		if(errConnection.size()>0){
			for (int i = 0; i < nodes.length; i++) {
				RouteResultsetNode node = nodes[i];
				final BackendConnection conn = session.getTarget(node);
				if(errConnection.contains(conn)){
					session.getTargetMap().remove(node); 
				}
			}
		}
		if(TxOperation.ROLLBACK.equals(txOperation)){
			session.rollback(this);
		}else{
			session.commit(this);
		}
	}
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		RouteResultsetNode node = (RouteResultsetNode)conn.getAttachment();
		session.getTargetMap().remove(node);
		if (!isFail)
			isFail = true;
		if (error != null) {
			error = new ErrorPacket();
			error.errno = ErrorCode.ER_NEW_ABORTING_CONNECTION;
			error.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());
		}
		logger.warn(new StringBuilder().append(conn.toString()).append(txOperation.toString()).append(" error : ")
				.append(error.message).toString());
		if (nodeCount.decrementAndGet() == 0) {
			error.write(session.getSource());
			if(errConnection.size()>0)
				session.getSource().close(closeReason);
		}
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		session.releaseConnectionIfSafe(conn, logger.isDebugEnabled(), false);
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(err);
		if (!isFail)
			isFail = true;
		logger.warn(new StringBuilder().append(conn.toString()).append(" ").append(txOperation.toString())
				.append(" error : ").append(errPacket.message.toString()));
		error = errPacket;
		if (nodeCount.decrementAndGet() == 0) {
			error.write(session.getSource());
			if(errConnection.size()>0)
				session.getSource().close(closeReason);
		}
		
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		session.releaseConnectionIfSafe(conn, logger.isDebugEnabled(), false);
		if (nodeCount.decrementAndGet() == 0) {
			if (isFail) {
				error.write(session.getSource());
			} else if (sendData == null) {
				OkPacket okPacket = new OkPacket();
				okPacket.read(ok);
				okPacket.write(session.getSource());
			} else {
				session.getSource().write(sendData);
			}
			if(errConnection.size()>0)
				session.getSource().close(closeReason);
		}
		
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		logger.warn(new StringBuilder().append("unexpected packet for ").append(conn).append(" bound by ")
				.append(session.getSource()).append(" ").append(txOperation.toString()).append(": field's eof")
				.toString()); 
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		logger.warn(new StringBuilder().append("unexpected packet for ").append(conn).append(" bound by ")
				.append(session.getSource()).append(" ").append(txOperation.toString()).append(": row data packet")
				.toString());
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		logger.warn(new StringBuilder().append("unexpected packet for ").append(conn).append(" bound by ")
				.append(session.getSource()).append(" ").append(txOperation.toString()).append(": row eof packet")
				.toString());
		
	}

	@Override
	public void writeQueueAvailable() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		RouteResultsetNode node = (RouteResultsetNode)conn.getAttachment();
		session.getTargetMap().remove(node);
		if (!isFail)
			isFail = true;
		if (error != null) {
			error = new ErrorPacket();
			error.errno = ErrorCode.ER_ABORTING_CONNECTION;
			error.message = StringUtil.encode(reason, session.getSource().getCharset());
		}
		logger.warn(new StringBuilder().append(conn.toString()).append(txOperation.toString()).append(" error : ")
				.append(error.message.toString()));
		if (nodeCount.decrementAndGet() == 0) {
			error.write(session.getSource());
			if(errConnection.size()>0)
				session.getSource().close(closeReason);
		}
		
	}
	
	
}
