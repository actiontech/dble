/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.subquery;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemNull;
import com.oceanbase.obsharding_d.plan.common.item.ItemString;
import com.oceanbase.obsharding_d.plan.common.item.subquery.UpdateItemSubQuery;
import com.oceanbase.obsharding_d.plan.node.ManagerTableNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class UpdateSubQueryHandler extends SubQueryHandler {

    public static final String NEED_REPLACE = "{CHILD}";
    private long maxRowsSize;
    private long rowCount = 0;
    private List<Field> sourceFieldList;
    private UpdateItemSubQuery itemSubQuery;

    public UpdateSubQueryHandler(long id, Session session, UpdateItemSubQuery itemSubQuery, boolean isExplain) {
        super(id, session);
        this.itemSubQuery = itemSubQuery;
        this.maxRowsSize = SystemConfig.getInstance().getQueryForUpdateMaxRowsSize();
        if (isExplain) {
            setForExplain();
        }
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            // create field for first time
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                sourceFieldList = HandlerTool.createFields(this.fieldPackets);

                int i = 0;
                for (Item item : itemSubQuery.getSelect()) {
                    PlanNode planNode = itemSubQuery.getPlanNode();
                    if (!(planNode instanceof ManagerTableNode) || ((ManagerTableNode) planNode).isNeedSendMaker()) {
                        item.setPushDownName(item.getAlias());
                    }
                    item.setTableName(sourceFieldList.get(i).getTable());
                    Item tmpItem = HandlerTool.createItem(item, Collections.singletonList(sourceFieldList.get(i)), 0, isAllPushDown(), type());
                    itemSubQuery.getField().add(tmpItem);
                    i++;
                }
                itemSubQuery.setSelect(itemSubQuery.getField());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        lock.lock();
        try {
            if (terminate.get()) {
                return true;
            }
            if (++rowCount > maxRowsSize) {
                String errMessage = "update involves too many rows in query,the maximum number of rows [queryForUpdateMaxRowsSize in bootstrap.cnf] allowed is " + maxRowsSize;
                LOGGER.info(errMessage);
                genErrorPackage(ErrorCode.ER_UNKNOWN_ERROR, errMessage);
                try {
                    tempDoneCallBack.call();
                } catch (Exception callback) {
                    LOGGER.info("callback exception!", callback);
                }
                return true;
            }
            RowDataPacket row = rowPacket;
            if (row == null) {
                row = new RowDataPacket(this.fieldPackets.size());
                row.read(rowNull);
            }
            int i = 0;
            for (byte[] fieldValue : row.getFieldValues()) {
                sourceFieldList.get(i).setPtr(fieldValue);
                i++;
            }
            List<Item> valueList = Lists.newArrayList();
            for (Item item : itemSubQuery.getField()) {
                if (item != null) {
                    Item resultItem = item.getResultItem();
                    valueList.add(resultItem == null ? new ItemNull() : resultItem);
                }
            }
            itemSubQuery.getValue().add(valueList);
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public HandlerType type() {
        return HandlerType.UPDATE_QUERY;
    }

    @Override
    public void setForExplain() {
        List<Item> valueItemList = Lists.newArrayList();
        for (Item ignored : itemSubQuery.getSelect()) {
            valueItemList.add(new ItemString(NEED_REPLACE, itemSubQuery.getCharsetIndex()));
        }
        itemSubQuery.getValue().add(valueItemList);
    }


    @Override
    public void clearForExplain() {
        itemSubQuery.getValue().clear();
    }


    @Override
    public ExplainType explainType() {
        return ExplainType.TYPE_UPDATE_SUB_QUERY;
    }

}
