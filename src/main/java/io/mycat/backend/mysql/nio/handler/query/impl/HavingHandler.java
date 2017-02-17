package io.mycat.backend.mysql.nio.handler.query.impl;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;

/**
 * 目前having做成和where一样的处理
 * 
 * @author chenzifei
 * 
 */
public class HavingHandler extends BaseDMLHandler {
	private static final Logger logger = Logger.getLogger(HavingHandler.class);

	public HavingHandler(long id, NonBlockingSession session, Item having) {
		super(id, session);
		assert (having != null);
		this.having = having;
	}

	private Item having = null;
	private Item havingItem = null;
	private List<Field> sourceFields;
	private ReentrantLock lock = new ReentrantLock();

	@Override
	public HandlerType type() {
		return HandlerType.HAVING;
	}

	/**
	 * 所有的上一级表传递过来的信息全部视作Field类型
	 */
	public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
			byte[] eofnull, boolean isLeft, BackendConnection conn) {
		if (terminate.get())
			return;
		this.fieldPackets = fieldPackets;
		this.sourceFields = HandlerTool.createFields(this.fieldPackets);
		/**
		 * having的函数我们基本算他不下发，因为他有可能带group by
		 */
		this.havingItem = HandlerTool.createItem(this.having, this.sourceFields, 0, false, this.type(),
				conn.getCharset());
		nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
	}

	/**
	 * 收到行数据包的响应处理
	 */
	public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
		if (terminate.get())
			return true;
		lock.lock();
		try {
			HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
			/* 根据where条件进行过滤 */
			if (havingItem.valBool()) {
				nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
			} else {
				// nothing
			}
			return false;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
		logger.debug("roweof");
		if (terminate.get())
			return;
		nextHandler.rowEofResponse(data, isLeft, conn);
	}

	@Override
	public void onTerminate() {
	}
}
