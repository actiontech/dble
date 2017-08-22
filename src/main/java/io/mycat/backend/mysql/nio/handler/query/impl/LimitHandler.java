package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用来处理limit，仅作简单的统计过滤
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
