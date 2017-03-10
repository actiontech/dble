package io.mycat.backend.mysql.store;

import java.util.Collections;

import io.mycat.backend.mysql.nio.handler.util.RowDataComparator;
import io.mycat.backend.mysql.store.diskbuffer.SortedResultDiskBuffer;
import io.mycat.backend.mysql.store.result.ResultExternal;
import io.mycat.buffer.BufferPool;

public class SortedLocalResult extends LocalResult {

	protected RowDataComparator rowcmp;

	public SortedLocalResult(BufferPool pool, int fieldsCount, RowDataComparator rowcmp, String charset) {
		this(DEFAULT_INITIAL_CAPACITY, fieldsCount, pool, rowcmp, charset);
	}

	public SortedLocalResult(int initialCapacity, int fieldsCount, BufferPool pool, RowDataComparator rowcmp,
			String charset) {
		super(initialCapacity, fieldsCount, pool, charset);
		this.rowcmp = rowcmp;
	}

	@Override
	protected ResultExternal makeExternal() {
		return new SortedResultDiskBuffer(pool, fieldsCount, rowcmp, charset);
	}

	@Override
	protected void beforeFlushRows() {
		Collections.sort(rows, this.rowcmp);
	}

	@Override
	protected void doneOnlyMemory() {
		Collections.sort(rows, this.rowcmp);
	}

}
