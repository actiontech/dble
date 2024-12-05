/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;


import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * if the Item is Item_sum,then theItem must be generated in GroupBy.Otherwise,calc by middle-ware
 */
public class SendMakeHandler extends BaseDMLHandler {

    private final ReentrantLock lock;

    private List<Item> selects;
    private List<Field> sourceFields;
    private List<Item> selItems;
    private String tableAlias;
    private String table;
    private String schema;
    private Set<BaseDMLHandler> tableHandlers;

    public SendMakeHandler(long id, Session session, List<Item> selects, String schema, String table, String tableAlias) {
        super(id, session);
        lock = new ReentrantLock();
        this.selects = selects;
        this.selItems = new ArrayList<>();
        this.schema = schema;
        this.table = table;
        this.tableAlias = tableAlias;
        this.tableHandlers = new HashSet<>();
    }

    @Override
    public HandlerType type() {
        return HandlerType.SENDMAKER;
    }


    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            session.setHandlerStart(this);
            if (terminate.get())
                return;
            this.fieldPackets = fieldPackets;
            this.sourceFields = HandlerTool.createFields(this.fieldPackets);
            for (Item sel : selects) {
                Item tmpItem = HandlerTool.createItem(sel, this.sourceFields, 0, isAllPushDown(), type());
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
            for (BaseDMLHandler tableHandler : tableHandlers) {
                tableHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, service);
            }
            nextHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
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
            for (BaseDMLHandler tableHandler : tableHandlers) {
                tableHandler.rowResponse(null, newRp, this.isLeft, service);
            }
            nextHandler.rowResponse(null, newRp, this.isLeft, service);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            session.setHandlerEnd(this);
            for (BaseDMLHandler tableHandler : tableHandlers) {
                tableHandler.rowEofResponse(eof, this.isLeft, service);
            }
            if (!tableHandlers.isEmpty()) {
                HandlerTool.terminateHandlerTree(this);
            }
            nextHandler.rowEofResponse(eof, this.isLeft, service);

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onTerminate() {
    }

    public Set<BaseDMLHandler> getTableHandlers() {
        return tableHandlers;
    }

    public void cleanBuffer() {
        if (nextHandler instanceof OutputHandler) {
            ((OutputHandler) nextHandler).cleanBuffer();
        }
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.SHUFFLE_FIELD;
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        nextHandler.okResponse(ok, service);
    }
}
