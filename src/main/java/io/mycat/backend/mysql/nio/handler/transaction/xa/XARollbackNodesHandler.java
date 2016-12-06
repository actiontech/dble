/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio.handler.transaction.xa;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.transaction.AbstractRollbackNodesHandler;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public class XARollbackNodesHandler extends AbstractRollbackNodesHandler{
	public XARollbackNodesHandler(NonBlockingSession session) {
		super(session);
	}
	
	@Override
	public void resetResponseHandler() {
		responsehandler = XARollbackNodesHandler.this;
	}
	@Override
	protected void endPhase(MySQLConnection mysqlCon) {
		if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
			String xaTxId = session.getXaTXID();
			mysqlCon.setXaStatus(TxState.TX_ENDED_STATE);
			mysqlCon.execCmd("XA END " + xaTxId + ";");
		}
	}

	@Override
	protected void rollbackPhase(MySQLConnection mysqlCon) {
		String xaTxId = session.getXaTXID();
		mysqlCon.setXaStatus(TxState.TX_ROLLBACKED_STATE);
		mysqlCon.execCmd("XA ROLLBACK " + xaTxId + ";");
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' ok
			case TX_ENDED_STATE:
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_ENDED_STATE);
					rollback();
				}
				return;
			// 'xa rollback' ok
			case TX_ROLLBACKED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(ok);
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(err);
		String errmsg = new String(errPacket.message);
		this.setFail(errmsg);
		conn.quit();
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' err
			case TX_ENDED_STATE:
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_ENDED_STATE);
					rollback();
				}
				return;
			// 'xa rollback' err
			case TX_ROLLBACKED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(errPacket.toBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.warn("backend connect", e);
		String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset()));
		this.setFail(errmsg);
		conn.quit();
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' err
			case TX_ENDED_STATE:
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_ENDED_STATE);
					rollback();
				}
				return;
			// 'xa rollback' err
			case TX_ROLLBACKED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(errmsg.getBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		this.setFail(reason);
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' err
			case TX_ENDED_STATE:
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_ENDED_STATE);
					rollback();
				}
				return;
			// 'xa rollback' err
			case TX_ROLLBACKED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(reason.getBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}
}
