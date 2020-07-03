/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.manager;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.util.StringUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * if the Item is Item_sum,then theItem must be generated in GroupBy.Otherwise,calc by middle-ware
 */
public class ManagerSendMakeHandler extends BaseDMLHandler {

    private final ReentrantLock lock;

    private List<Item> selects;
    private List<Field> sourceFields;
    private List<Item> selItems;
    private String tableAlias;
    private String table;
    private String schema;

    public ManagerSendMakeHandler(long id, Session session, List<Item> selects, String schema, String table, String tableAlias) {
        super(id, session);
        lock = new ReentrantLock();
        this.selects = selects;
        this.selItems = new ArrayList<>();
        this.schema = schema;
        this.table = table;
        this.tableAlias = tableAlias;
    }

    @Override
    public HandlerType type() {
        return HandlerType.MANAGER_SENDMAKER;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
        lock.lock();
        try {
            session.setHandlerStart(this);
            if (terminate.get())
                return;
            this.fieldPackets = fieldPackets;
            this.sourceFields = HandlerTool.createFields(this.fieldPackets);
            for (Item sel : selects) {
                Item tmpItem = HandlerTool.createManagerItem(sel, this.sourceFields, 0, isAllPushDown(), type());
                tmpItem.setItemName(sel.getItemName());
                String selAlias = sel.getAlias();
                if (selAlias != null) {
                    // remove the added tmp FNAF
                    selAlias = StringUtil.removeApostropheOrBackQuote(selAlias);
                    if (StringUtils.indexOf(selAlias, Item.FNAF) == 0)
                        selAlias = StringUtils.substring(selAlias, Item.FNAF.length());
                }
                tmpItem = HandlerTool.createRefItem(tmpItem, schema, table, tableAlias, selAlias);
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
            nextHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
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
            nextHandler.rowResponse(null, newRp, this.isLeft, service);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            session.setHandlerEnd(this);
            nextHandler.rowEofResponse(eof, this.isLeft, service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onTerminate() {
    }


}
