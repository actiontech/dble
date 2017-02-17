package io.mycat.plan.common.external;

import io.mycat.net.mysql.RowDataPacket;

public interface ResultStore {
	/* add a new row */
	void add(RowDataPacket row);

	/* all rows added */
	void done();

	/* visit all rows in the store */
	RowDataPacket next();

	int getRowCount();

	/* 关闭result */
	void close();

	/* 清楚数据 */
	void clear();
}
