package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * having is just as same as where
 *
 * @author ActionTech
 */
public class HavingHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(HavingHandler.class);

    public HavingHandler(long id, NonBlockingSession session, Item having) {
        super(id, session);
        assert (having != null);
        this.having = having;
    }

    private Item having = null;
    private Item havingItem = null;
    private List<Field> sourceFields;
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.HAVING;
    }

    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        /**
         * having will not be pushed down because of aggregate function
         */
        this.havingItem = HandlerTool.createItem(this.having, this.sourceFields, 0, false, this.type(),
                conn.getCharset());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            /* filter by having statement */
            if (havingItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        nextHandler.rowEofResponse(data, isLeft, conn);
    }

    @Override
    public void onTerminate() {
    }
}
