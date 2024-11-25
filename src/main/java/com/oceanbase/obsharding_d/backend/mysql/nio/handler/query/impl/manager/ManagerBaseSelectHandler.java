/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.manager;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemInt;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;
import com.oceanbase.obsharding_d.plan.node.ManagerTableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.services.FakeResponseService;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

public class ManagerBaseSelectHandler extends BaseDMLHandler {
    private static Logger logger = LoggerFactory.getLogger(ManagerBaseSelectHandler.class);
    private ManagerTableNode tableNode;
    private boolean needSendMaker = false;
    private LinkedHashSet<Item> realSelects;

    public ManagerBaseSelectHandler(long id, Session session, ManagerTableNode tableNode) {
        super(id, session);
        this.tableNode = tableNode;
        this.merges.add(this);
        this.realSelects = getBaseItems();
    }

    public void execute() {

        final BaseDMLHandler nextHandler = this.nextHandler;
        final boolean left = this.isLeft;
        OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    final MySQLResponseService service = new FakeResponseService(null);
                    List<FieldPacket> fields = makeField();
                    nextHandler.fieldEofResponse(null, null, fields, null, left, service);
                    List<RowDataPacket> data = makeRowData();
                    for (RowDataPacket row : data) {
                        nextHandler.rowResponse(null, row, left, service);
                    }
                    nextHandler.rowEofResponse(null, left, service);
                } catch (Exception e) {
                    logger.warn("execute error", e);
                    ((ManagerService) session.getSource().getService()).writeErrMessage((byte) 1, ErrorCode.ER_UNKNOWN_ERROR, e.getMessage() == null ? e.toString() : e.getMessage());
                }
            }
        });
    }


    private List<FieldPacket> makeField() {
        List<FieldPacket> totalResult = new ArrayList<>();
        for (Item select : realSelects) {
            int type = Fields.FIELD_TYPE_VAR_STRING;
            if (!select.basicConstItem()) {
                type = ManagerSchemaInfo.getInstance().getTables().get(tableNode.getTableName()).getColumnType(select.getItemName());
            }
            FieldPacket field = PacketUtil.getField(select.getItemName(), type);
            field.setDb(ManagerSchemaInfo.SCHEMA_NAME.getBytes());
            if (tableNode.getAlias() == null) {
                field.setTable(tableNode.getTableName().getBytes());
            } else {
                field.setTable(tableNode.getAlias().getBytes());
                field.setOrgTable(tableNode.getTableName().getBytes());
            }
            totalResult.add(field);
        }
        return totalResult;
    }

    private LinkedHashSet<Item> getBaseItems() {
        LinkedHashSet<Item> realSelectItem = new LinkedHashSet<>(tableNode.getColumnsSelected().size());
        for (Item select : tableNode.getColumnsSelected()) {
            realSelectItem.addAll(getBaseItem(select, true));
            if (select.getAlias() != null && !select.getAlias().equals(select.getItemName())) {
                needSendMaker = true;
            }
        }
        for (Order orderItem : tableNode.getOrderBys()) {
            int size = realSelectItem.size();
            realSelectItem.addAll(getBaseItem(orderItem.getItem(), false));
            if (!needSendMaker && realSelectItem.size() > size) {
                needSendMaker = true;
            }
        }
        for (Order groupByItem : tableNode.getGroupBys()) {
            int size = realSelectItem.size();
            realSelectItem.addAll(getBaseItem(groupByItem.getItem(), false));
            if (!needSendMaker && realSelectItem.size() > size) {
                needSendMaker = true;
            }
        }
        if (tableNode.getWhereFilter() != null) {
            int size = realSelectItem.size();
            realSelectItem.addAll(getBaseItem(tableNode.getWhereFilter(), false));
            if (!needSendMaker && realSelectItem.size() > size) {
                needSendMaker = true;
            }
        }
        if (tableNode.getHavingFilter() != null) {
            int size = realSelectItem.size();
            realSelectItem.addAll(getBaseItem(tableNode.getHavingFilter(), false));
            if (!needSendMaker && realSelectItem.size() > size) {
                needSendMaker = true;
            }
        }
        return realSelectItem;
    }

    private List<Item> getBaseItem(Item select, boolean isRealSelect) {
        if (select.isWithSubQuery()) {
            return getBaseItem(PlanUtil.rebuildSubQueryItem(select), isRealSelect);
        }
        if (select.basicConstItem()) {
            return isRealSelect ? Collections.singletonList(select) : Collections.emptyList();
        }
        Item.ItemType i = select.type();
        if ((i == Item.ItemType.FUNC_ITEM) || (i == Item.ItemType.COND_ITEM)) {
            ItemFunc func = (ItemFunc) select;
            if (isRealSelect) {
                needSendMaker = true;
            }
            return createFunctionItem(func, isRealSelect);

        } else if (i == Item.ItemType.SUM_FUNC_ITEM) {
            ItemSum sumFunc = (ItemSum) select;
            if (isRealSelect) {
                needSendMaker = true;
            }
            return createSumItem(sumFunc, isRealSelect);
        } else {
            select.setItemName(select.getItemName().toLowerCase());
            return Collections.singletonList(select);
        }
    }

    private List<Item> createFunctionItem(ItemFunc f, boolean isRealSelect) {
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            if (arg.isWild()) {
                args.add(new ItemInt(0));
            } else {
                args.addAll(getBaseItem(arg, isRealSelect));
            }

        }
        return args;
    }

    private List<Item> createSumItem(ItemSum f, boolean isRealSelect) {
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            if (arg.isWild()) {
                args.add(new ItemInt(0));
            } else {
                args.addAll(getBaseItem(arg, isRealSelect));
            }
        }
        return args;
    }


    public boolean isNeedSendMaker() {
        return needSendMaker;
    }

    private List<RowDataPacket> makeRowData() {
        ManagerBaseTable table = ManagerSchemaInfo.getInstance().getTables().get(tableNode.getTableName());
        return table.getRow(realSelects, session.getSource().getService().getCharset().getResults());

    }

    @Override
    public HandlerType type() {
        return null;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {

    }

    @Override
    protected void onTerminate() throws Exception {

    }
}
