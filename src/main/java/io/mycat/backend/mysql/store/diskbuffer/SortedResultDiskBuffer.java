package io.mycat.backend.mysql.store.diskbuffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import io.mycat.backend.mysql.nio.handler.util.ArrayMinHeap;
import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.buffer.BufferPool;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.MinHeap;
import io.mycat.util.TimeUtil;

/**
 * sort need diskbuffer, when done() is called,users use next() to get the
 * result rows which have been sorted already
 * 
 * @author ActionTech
 * 
 */
public class SortedResultDiskBuffer extends ResultDiskBuffer {
	private final Logger logger = Logger.getLogger(SortedResultDiskBuffer.class);

	/**
	 * the tapes to store data, which is sorted each, so we can use minheap to
	 * sort them
	 */
	protected final ArrayList<ResultDiskTape> tapes;
	/**
	 * the sort cmptor
	 */
	private final RowDataComparator comparator;
	/**
	 * the heap used for sorting the sorted tapes
	 */
	protected MinHeap<TapeItem> heap;
	protected Comparator<TapeItem> heapCmp;

	public SortedResultDiskBuffer(BufferPool pool, int columnCount, RowDataComparator cmp, String charset) {
		super(pool, columnCount, charset);
		tapes = new ArrayList<ResultDiskTape>();
		this.comparator = cmp;
		this.heapCmp = new Comparator<TapeItem>() {
			@Override
			public int compare(TapeItem o1, TapeItem o2) {
				RowDataPacket row1 = o1.row;
				RowDataPacket row2 = o2.row;
				if (row1 == null || row2 == null) {
					if (row1 == row2)
						return 0;
					if (row1 == null)
						return -1;
					return 1;
				}
				return comparator.compare(row1, row2);
			}
		};
	}

	@Override
	public final int TapeCount() {
		return tapes.size();
	}

	@Override
	public final int addRows(List<RowDataPacket> rows) {
		/**
		 * we should make rows sorted first, then write them into file
		 */
		if (logger.isDebugEnabled()) {
			logger.debug(" convert list to array start:" + TimeUtil.currentTimeMillis());
		}
		RowDataPacket[] rowArray = new RowDataPacket[rows.size()];
		rows.toArray(rowArray);
		long start = file.getFilePointer();
		for (RowDataPacket row : rowArray) {
			byte[] b = row.toBytes();
			writeBuffer = writeToBuffer(b, writeBuffer);
		}
		// help for gc
		rowArray = null;
		writeBuffer.flip();
		file.write(writeBuffer);
		writeBuffer.clear();
		/* make a new tape */
		ResultDiskTape tape = makeResultDiskTape();
		tape.start = start;
		tape.filePos = start;
		tape.end = file.getFilePointer();
		tapes.add(tape);
		rowCount += rows.size();
		if (logger.isDebugEnabled()) {
			logger.debug("write rows to disk end:" + TimeUtil.currentTimeMillis());
		}
		return rowCount;
	}

	/**
	 * to override by group by
	 * 
	 * @return
	 */
	protected ResultDiskTape makeResultDiskTape() {
		return new ResultDiskTape(pool, file, columnCount);
	}

	@Override
	public RowDataPacket next() {
		if (heap.isEmpty())
			return null;
		TapeItem tapeItem = heap.poll();
		RowDataPacket newRow = tapeItem.tape.nextRow();
		if (newRow != null) {
			heap.add(new TapeItem(newRow, tapeItem.tape));
		}
		return tapeItem.row;
	}

	@Override
	public final void reset() {
		for (ResultDiskTape tape : tapes) {
			tape.filePos = tape.start;
			tape.pos = tape.start;
			tape.readBufferOffset = 0;
			tape.readBuffer.clear();
		}
		resetHeap();
	}

	protected void resetHeap() {
		if (heap == null)
			heap = new ArrayMinHeap<TapeItem>(tapes.size(), this.heapCmp);
		heap.clear();
		// init heap
		for (int i = 0; i < tapes.size(); i++) {
			heap.add(new TapeItem(tapes.get(i).nextRow(), tapes.get(i)));
		}
	}

}
