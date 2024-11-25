/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LimitHandler
 *
 * @author ActionTech
 */
public class LimitHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(LimitHandler.class);
    private long limitIndex;
    private final long limitCount;
    // current index
    private AtomicLong curIndex = new AtomicLong(-1L);

    public LimitHandler(long id, Session session, long limitIndex, long limitCount) {
        super(id, session);
        this.limitIndex = limitIndex;
        this.limitCount = limitCount;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.debug("row eof");
        if (!terminate.get()) {
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(data, this.isLeft, service);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.LIMIT;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get()) {
            return true;
        }
        long curIndexTmp = curIndex.incrementAndGet();
        if (curIndexTmp < limitIndex) {
            return false;
        } else if (curIndexTmp < limitIndex + limitCount) {
            nextHandler.rowResponse(null, rowPacket, this.isLeft, service);
        } else {
            return true;
        }
        return false;
    }

    @Override
    protected void onTerminate() {
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.LIMIT;
    }
}
