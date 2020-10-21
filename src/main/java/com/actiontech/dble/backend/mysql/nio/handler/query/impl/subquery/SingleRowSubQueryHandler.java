/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery;


import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemString;
import com.actiontech.dble.plan.common.item.subquery.ItemSingleRowSubQuery;
import com.actiontech.dble.plan.node.ManagerTableNode;
import com.actiontech.dble.plan.node.PlanNode;

import java.util.Collections;
import java.util.List;

import static com.actiontech.dble.plan.optimizer.JoinStrategyProcessor.NEED_REPLACE;

public class SingleRowSubQueryHandler extends SubQueryHandler {
    private int rowCount = 0;
    private Field sourceField;
    private ItemSingleRowSubQuery itemSubQuery;
    public SingleRowSubQueryHandler(long id, Session session, ItemSingleRowSubQuery itemSubQuery) {
        super(id, session);
        this.itemSubQuery = itemSubQuery;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            // create field for first time
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                sourceField = HandlerTool.createField(this.fieldPackets.get(0));
                if (itemSubQuery.isField()) {
                    setSubQueryFiled();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        lock.lock();
        try {
            if (terminate.get()) {
                return true;
            }
            if (++rowCount > 1) {
                String errMessage = "Subquery returns more than 1 row";
                LOGGER.info(errMessage);
                genErrorPackage(ErrorCode.ER_SUBQUERY_NO_1_ROW, errMessage);
                return true;
            }
            RowDataPacket row = rowPacket;
            if (row == null) {
                row = new RowDataPacket(this.fieldPackets.size());
                row.read(rowNull);
            }
            if (!itemSubQuery.isField()) {
                setSubQueryFiled();
            }
            sourceField.setPtr(row.getValue(0));
        } finally {
            lock.unlock();
        }
        return false;
    }


    @Override
    public HandlerType type() {
        return HandlerType.SCALAR_SUB_QUERY;
    }

    private void setSubQueryFiled() {
        Item select = itemSubQuery.getSelect();
        PlanNode planNode = itemSubQuery.getPlanNode();
        if (!(planNode instanceof ManagerTableNode) || ((ManagerTableNode) planNode).isNeedSendMaker()) {
            select.setPushDownName(select.getAlias());
        }
        Item tmpItem = HandlerTool.createItem(select, Collections.singletonList(this.sourceField), 0, isAllPushDown(), type());
        itemSubQuery.setValue(tmpItem);
    }

    @Override
    public void setForExplain() {
        itemSubQuery.setValue(new ItemString(NEED_REPLACE, itemSubQuery.getCharsetIndex()));
    }
}
