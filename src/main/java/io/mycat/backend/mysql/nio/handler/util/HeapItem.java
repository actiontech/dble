package io.mycat.backend.mysql.nio.handler.util;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.net.mysql.RowDataPacket;

public class HeapItem {
	private byte[] row;
	private RowDataPacket rowPacket;
	private MySQLConnection hashIndex;
	private boolean isNullItem = false;

	public static HeapItem NULLITEM() {
		HeapItem NULLITEM = new HeapItem(null, null, null);
		NULLITEM.isNullItem = true;
		return NULLITEM;
	}

	public boolean IsNullItem() {
		if (row == null && isNullItem == true)
			return true;
		return false;
	}

	public HeapItem(byte[] row, RowDataPacket rdp, MySQLConnection index) {
		this.row = row;
		this.rowPacket = rdp;
		this.hashIndex = index;
	}

	public MySQLConnection getIndex() {
		return hashIndex;
	}

	public byte[] getRowData() {
		return row;
	}

	public RowDataPacket getRowPacket() {
		return this.rowPacket;
	}

}
