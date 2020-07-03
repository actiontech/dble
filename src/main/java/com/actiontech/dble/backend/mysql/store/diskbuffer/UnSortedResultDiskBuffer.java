/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.diskbuffer;

import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * no sort need diskbuffer,when a new row come in,added it directly
 *
 * @author ActionTech
 */
public class UnSortedResultDiskBuffer extends ResultDiskBuffer {
    private final Logger logger = LoggerFactory.getLogger(UnSortedResultDiskBuffer.class);
    /**
     * the tape to store unsorted data
     */
    private final ResultDiskTape mainTape;

    public UnSortedResultDiskBuffer(BufferPool pool, int columnCount) {
        super(pool, columnCount);
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
            logger.debug("writeDirectly rows to disk end:" + TimeUtil.currentTimeMillis());
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
