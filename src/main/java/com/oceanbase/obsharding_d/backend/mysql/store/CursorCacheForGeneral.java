/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store;

import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.external.ResultStore;
import com.oceanbase.obsharding_d.singleton.BufferPoolManager;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author dcy
 * Create Date: 2020-12-22
 */
public class CursorCacheForGeneral implements CursorCache {
    private final ResultStore localResult;
    private volatile boolean complete = false;


    public CursorCacheForGeneral(int fieldCount, BufferPoolRecord.Builder bufferRecordBuilder) {
        final UnSortedLocalResult unSortedLocalResult = new UnSortedLocalResult(fieldCount, BufferPoolManager.getBufferPool(), CHARSET, bufferRecordBuilder);
        /*
            max-memory before persist.
         */
        unSortedLocalResult.setMaxMemory(SystemConfig.getInstance().getMaxHeapTableSize());
        /*
            read buffer chunk size
         */
        unSortedLocalResult.setMaxReadMemorySize(SystemConfig.getInstance().getHeapTableBufferChunkSize());
        this.localResult = unSortedLocalResult;

    }


    @Override
    public void add(RowDataPacket row) {
        localResult.add(row);
    }


    @Override
    public void done() {
        localResult.done();
        complete = true;
    }


    @Override
    public boolean isDone() {
        return complete;
    }


    @Override
    public Iterator<RowDataPacket> fetchBatch(long expectRowNum) {
        return new ScannerIterator(expectRowNum);
    }

    @Override
    public int getRowCount() {
        return localResult.getRowCount();
    }


    @Override
    public void close() {
        localResult.close();
    }

    private final class ScannerIterator implements Iterator<RowDataPacket> {
        private RowDataPacket nextPacket;
        private boolean fetched = false;
        long readCount = 0;
        long limitCount;

        private ScannerIterator(long limitCount) {
            this.limitCount = limitCount;
        }

        @Override
        public boolean hasNext() {
            if (fetched) {
                return true;
            }
            if (readCount >= limitCount) {
                return false;
            }
            nextPacket = localResult.next();
            fetched = true;
            readCount++;
            return nextPacket != null;
        }

        @Override
        public RowDataPacket next() {
            if (!fetched) {
                throw new NoSuchElementException("please call hasNext()  before this.");
            }
            fetched = false;
            return nextPacket;
        }
    }
}
