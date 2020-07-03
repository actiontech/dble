package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;

import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.net.Session;

import java.util.List;

/**
 * Created by szf on 2019/5/29.
 */
public class MultiNodeFakeHandler extends MultiNodeMergeHandler {

    private FakeBaseSelectHandler execHandler;

    public MultiNodeFakeHandler(long id, Session session, List<Item> selectList, boolean partOfUnion) {
        super(id, session);
        this.execHandler = new FakeBaseSelectHandler(id, session, selectList, this, partOfUnion);
        this.merges.add(this);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        session.setHandlerStart(this);
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        if (terminate.get())
            return true;
        return nextHandler.rowResponse(null, rowPacket, this.isLeft, service);
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        if (this.terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(null, this.isLeft, service);
    }

    @Override
    public void execute() throws Exception {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                execHandler.fakeExecute();
            }
        });
    }

    @Override
    protected void ownThreadJob(Object... objects) {

    }

    @Override
    protected void terminateThread() throws Exception {

    }

    @Override
    protected void recycleResources() {

    }

    public String toSQLString() {
        return execHandler.toSQLString();
    }


    @Override
    public HandlerType type() {
        return HandlerType.FAKE_MERGE;
    }
}
