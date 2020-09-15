/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.net.Session;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class WhereHandler extends BaseDMLHandler {

    public WhereHandler(long id, Session session, Item where) {
        super(id, session);
        assert (where != null);
        this.where = where;
    }

    private Item where = null;
    private List<Field> sourceFields;
    // if merge handler have no order by, the row response is not thread safe
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.WHERE;
    }

    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, service);
    }

    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            Item whereItem = HandlerTool.createItem(this.where, this.sourceFields, 0, this.isAllPushDown(), this.type());
            /* use whereto filter */
            if (whereItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, service);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        if (terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(data, this.isLeft, service);
    }

    @Override
    public void onTerminate() {
    }


}
