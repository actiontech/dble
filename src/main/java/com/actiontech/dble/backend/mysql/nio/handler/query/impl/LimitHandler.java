/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LimitHandler
 *
 * @author ActionTech
 */
public class LimitHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(LimitHandler.class);
    private long limitIndex;
    private final long limitCount;
    // current index
    private AtomicLong curIndex = new AtomicLong(-1L);

    public LimitHandler(long id, NonBlockingSession session, long limitIndex, long limitCount) {
        super(id, session);
        this.limitIndex = limitIndex;
        this.limitCount = limitCount;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("row eof");
        if (!terminate.get()) {
            nextHandler.rowEofResponse(data, this.isLeft, conn);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.LIMIT;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return true;
        }
        long curIndexTmp = curIndex.incrementAndGet();
        if (curIndexTmp < limitIndex) {
            return false;
        } else if (curIndexTmp >= limitIndex && curIndexTmp < limitIndex + limitCount) {
            nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
        } else {
            return true;
        }
        return false;
    }

    @Override
    protected void onTerminate() {
    }
}
