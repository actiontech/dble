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
import com.oceanbase.obsharding_d.plan.common.item.ItemString;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemInSubQuery;
import com.oceanbase.obsharding_d.plan.node.ManagerTableNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

import static com.oceanbase.obsharding_d.plan.optimizer.JoinStrategyProcessor.NEED_REPLACE;

public class InSubQueryHandler extends SubQueryHandler {
    private int maxPartSize = 2000;
    private int maxConnSize = 4;
    private int rowCount = 0;
    private Field sourceField;
    private ItemInSubQuery itemSubQuery;

    public InSubQueryHandler(long id, Session session, ItemInSubQuery itemSubQuery, boolean isExplain) {
        super(id, session);
        this.itemSubQuery = itemSubQuery;
        this.maxPartSize = SystemConfig.getInstance().getNestLoopRowsSize();
        this.maxConnSize = SystemConfig.getInstance().getNestLoopConnSize();
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
                sourceField = HandlerTool.createField(this.fieldPackets.get(0));
                Item select = itemSubQuery.getSelect();
                PlanNode planNode = itemSubQuery.getPlanNode();
                if (!(planNode instanceof ManagerTableNode) || ((ManagerTableNode) planNode).isNeedSendMaker()) {
                    select.setPushDownName(select.getAlias());
                }
                Item tmpItem = HandlerTool.createItem(select, Collections.singletonList(this.sourceField), 0, isAllPushDown(), type());
                itemSubQuery.setFiled(tmpItem);
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
            if (++rowCount > maxPartSize * maxConnSize) {
                String errMessage = "sub query too much rows!";
                LOGGER.info(errMessage);
                genErrorPackage(ErrorCode.ER_UNKNOWN_ERROR, errMessage);
                service.getConnection().close(errMessage);
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
            sourceField.setPtr(row.getValue(0));
            Item value = itemSubQuery.getFiled().getResultItem();
            if (value == null) {
                itemSubQuery.setContainNull(true);
            } else {
                itemSubQuery.getValue().add(value);
            }
        } finally {
            lock.unlock();
        }
        return false;
    }


    @Override
    public HandlerType type() {
        return HandlerType.IN_SUB_QUERY;
    }

    @Override
    public void setForExplain() {
        itemSubQuery.getValue().add(new ItemString(NEED_REPLACE, itemSubQuery.getCharsetIndex()));
    }


    @Override
    public void clearForExplain() {
        itemSubQuery.getValue().clear();
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.IN_SUB_QUERY;
    }

}
