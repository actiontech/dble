package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class WhereHandler extends BaseDMLHandler {

    public WhereHandler(long id, NonBlockingSession session, Item where) {
        super(id, session);
        assert (where != null);
        this.where = where;
    }

    private Item where = null;
    private Item whereItem = null;
    private List<Field> sourceFields;
    // if merge handler have no order by, the row response is not thread safe
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.WHERE;
    }

    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        whereItem = HandlerTool.createItem(this.where, this.sourceFields, 0, this.isAllPushDown(), this.type(),
                conn.getCharset());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
    }

    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            /* use whereto filter */
            if (whereItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        nextHandler.rowEofResponse(data, isLeft, conn);
    }

    @Override
    public void onTerminate() {
    }


}
