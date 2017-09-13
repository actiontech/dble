/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * if the Item is Item_sum,then theItem must be generated in GroupBy.Otherwise,calc by middle-ware
 */
public class SendMakeHandler extends BaseDMLHandler {

    private final ReentrantLock lock;

    private List<Item> sels;
    private List<Field> sourceFields;
    private List<Item> selItems;
    private String tbAlias;

    /**
     * @param session
     * @param sels
     */
    public SendMakeHandler(long id, NonBlockingSession session, List<Item> sels, String tableAlias) {
        super(id, session);
        lock = new ReentrantLock();
        this.sels = sels;
        this.selItems = new ArrayList<>();
        this.tbAlias = tableAlias;
    }

    @Override
    public HandlerType type() {
        return HandlerType.SENDMAKER;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            this.fieldPackets = fieldPackets;
            this.sourceFields = HandlerTool.createFields(this.fieldPackets);
            for (Item sel : sels) {
                Item tmpItem = HandlerTool.createItem(sel, this.sourceFields, 0, isAllPushDown(), type());
                tmpItem.setItemName(sel.getItemName());
                if (sel.getAlias() != null || tbAlias != null) {
                    String selAlias = sel.getAlias();
                    // remove the added tmp FNAF
                    if (StringUtils.indexOf(selAlias, Item.FNAF) == 0)
                        selAlias = StringUtils.substring(selAlias, Item.FNAF.length());
                    tmpItem = HandlerTool.createRefItem(tmpItem, tbAlias, selAlias);
                }
                this.selItems.add(tmpItem);
            }
            List<FieldPacket> newFieldPackets = new ArrayList<>();
            for (Item selItem : this.selItems) {
                FieldPacket tmpFp = new FieldPacket();
                selItem.makeField(tmpFp);
                /* Keep things compatible for old clients */
                if (tmpFp.getType() == FieldTypes.MYSQL_TYPE_VARCHAR.numberValue())
                    tmpFp.setType(FieldTypes.MYSQL_TYPE_VAR_STRING.numberValue());
                newFieldPackets.add(tmpFp);
            }
            nextHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return true;
            HandlerTool.initFields(sourceFields, rowPacket.fieldValues);
            RowDataPacket newRp = new RowDataPacket(selItems.size());
            for (Item selItem : selItems) {
                byte[] b = selItem.getRowPacketByte();
                newRp.add(b);
            }
            nextHandler.rowResponse(null, newRp, this.isLeft, conn);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            nextHandler.rowEofResponse(eof, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onTerminate() {
    }


}
