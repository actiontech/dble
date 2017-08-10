package io.mycat.backend.mysql.store.diskbuffer;

import java.util.List;

import org.apache.log4j.Logger;

import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.TimeUtil;

/**
 * no sort need diskbuffer，when a new row come in,added it directly
 * 
 * @author ActionTech
 * 
 */
public class UnSortedResultDiskBuffer extends ResultDiskBuffer {
	private final Logger logger = Logger.getLogger(UnSortedResultDiskBuffer.class);
	/**
	 * the tape to store unsorted data
	 */
	private final ResultDiskTape mainTape;

	public UnSortedResultDiskBuffer(BufferPool pool, int columnCount, String charset) {
		super(pool, columnCount, charset);
		mainTape = new ResultDiskTape(pool, file, columnCount);
	}

	@Override
	public int tapeCount() {
		return 1;
	}

	@Override
	public int addRows(List<RowDataPacket> rows) {
		if (logger.isDebugEnabled()) {
			logger.debug("addRows start:" + TimeUtil.currentTimeMillis());
		}
		for (RowDataPacket row : rows) {
			byte[] b = row.toBytes();
			writeBuffer = writeToBuffer(b, writeBuffer);
		}
		writeBuffer.flip();
		file.write(writeBuffer);
		writeBuffer.clear();
		mainTape.end = file.getFilePointer();
		rowCount += rows.size();
		if (logger.isDebugEnabled()) {
			logger.debug("write rows to disk end:" + TimeUtil.currentTimeMillis());
		}
		return rowCount;
	}

	@Override
	public void reset() {
		mainTape.pos = mainTape.start;
		mainTape.filePos = mainTape.start;
		mainTape.readBufferOffset = 0;
		mainTape.readBuffer.clear();
	}

	@Override
	public RowDataPacket next() {
		file.seek(mainTape.pos);
		return mainTape.nextRow();
	}

}
