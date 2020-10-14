/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery;


import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemString;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.node.ManagerTableNode;
import com.actiontech.dble.plan.node.PlanNode;

import java.util.Collections;
import java.util.List;

public class AllAnySubQueryHandler extends SubQueryHandler {
    public static final String ALL_SUB_QUERY_RESULTS = "{ALL_SUB_QUERY_RESULTS}";
    public static final String MIN_SUB_QUERY_RESULTS = "{MIN_SUB_QUERY_RESULTS}";
    public static final String MAX_SUB_QUERY_RESULTS = "{MAX_SUB_QUERY_RESULTS}";
    private ItemAllAnySubQuery itemSubQuery;
    private Field sourceField;
    private RowDataPacket tmpRow;
    private RowDataComparator rowComparator;

    public AllAnySubQueryHandler(long id, Session session, ItemAllAnySubQuery itemSubQuery) {
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
                Item select = itemSubQuery.getSelect();
                PlanNode planNode = itemSubQuery.getPlanNode();
                if (!(planNode instanceof ManagerTableNode) || ((ManagerTableNode) planNode).isNeedSendMaker()) {
                    select.setPushDownName(select.getAlias());
                }
                Item tmpItem = HandlerTool.createItem(select, Collections.singletonList(this.sourceField), 0, isAllPushDown(), type());
                itemSubQuery.setFiled(tmpItem);
                rowComparator = new RowDataComparator(this.fieldPackets, Collections.singletonList(new Order(select)), this.isAllPushDown(), this.type());
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
            RowDataPacket row = rowPacket;
            if (row == null) {
                row = new RowDataPacket(this.fieldPackets.size());
                row.read(rowNull);
            }
            sourceField.setPtr(row.getValue(0));
            Item value = itemSubQuery.getFiled().getResultItem();
            if (value == null) { //null will not cmp with other value
                itemSubQuery.setContainNull(true);
                return true;
            }
            if (itemSubQuery.getValue().size() == 0) {
                itemSubQuery.getValue().add(value);
                tmpRow = row;
            } else if (itemSubQuery.getValue().size() == 1) {
                handleNewRow(row, value);
            } else {
                // ignore row
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public HandlerType type() {
        return HandlerType.ALL_ANY_SUB_QUERY;
    }

    private void handleNewRow(RowDataPacket row, Item value) {
        int result = rowComparator.compare(tmpRow, row);
        switch (itemSubQuery.getOperator()) {
            case Equality:
                if (itemSubQuery.isAll() && result != 0) {
                    itemSubQuery.getValue().add(value);
                }
                break;
            case NotEqual:
            case LessThanOrGreater:
                if (!itemSubQuery.isAll() && result != 0) {
                    itemSubQuery.getValue().add(value);
                }
                break;
            case LessThan:
            case LessThanOrEqual:
                if (itemSubQuery.isAll() && result > 0) {
                    //row < tmpRow
                    itemSubQuery.getValue().set(0, value);
                    tmpRow = row;
                } else if (!itemSubQuery.isAll() && result < 0) {
                    //row > tmpRow
                    itemSubQuery.getValue().set(0, value);
                    tmpRow = row;
                }
                break;
            case GreaterThan:
            case GreaterThanOrEqual:
                if (itemSubQuery.isAll() && result < 0) {
                    //row > tmpRow
                    itemSubQuery.getValue().set(0, value);
                    tmpRow = row;
                } else if (!itemSubQuery.isAll() && result > 0) {
                    //row < tmpRow
                    itemSubQuery.getValue().set(0, value);
                    tmpRow = row;
                }
                break;
            default:
                break;
        }
    }


    @Override
    public void setForExplain() {
        switch (itemSubQuery.getOperator()) {
            case Equality:
                if (itemSubQuery.isAll()) {
                    itemSubQuery.getValue().add(new ItemString(ALL_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                }
                break;
            case NotEqual:
            case LessThanOrGreater:
                if (!itemSubQuery.isAll()) {
                    itemSubQuery.getValue().add(new ItemString(ALL_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                }
                break;
            case LessThan:
            case LessThanOrEqual:
                if (itemSubQuery.isAll()) {
                    //row < tmpRow
                    itemSubQuery.getValue().add(new ItemString(MIN_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                } else if (!itemSubQuery.isAll()) {
                    //row > tmpRow
                    itemSubQuery.getValue().add(new ItemString(MAX_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                }
                break;
            case GreaterThan:
            case GreaterThanOrEqual:
                if (itemSubQuery.isAll()) {
                    //row > tmpRow
                    itemSubQuery.getValue().add(new ItemString(MAX_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                } else if (!itemSubQuery.isAll()) {
                    //row < tmpRow
                    itemSubQuery.getValue().add(new ItemString(MIN_SUB_QUERY_RESULTS, itemSubQuery.getCharsetIndex()));
                }
                break;
            default:
                break;
        }
    }
}
