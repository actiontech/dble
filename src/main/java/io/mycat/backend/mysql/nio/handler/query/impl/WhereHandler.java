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
    // 因为merge在没有order by时会存在多线程并发rowresponse
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.WHERE;
    }

    /**
     * 所有的上一级表传递过来的信息全部视作Field类型
     */
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

    /**
     * 收到行数据包的响应处理
     */
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            /* 根据where条件进行过滤 */
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

    /**
     * 收到行数据包结束的响应处理
     */
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        nextHandler.rowEofResponse(data, isLeft, conn);
    }

    @Override
    public void onTerminate() {
    }


}
