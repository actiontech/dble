package com.actiontech.dble.backend.mysql.nio.handler.query.impl;


import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFuncInner;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.net.Session;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2019/5/28.
 */
public class FakeBaseSelectHandler extends BaseDMLHandler {

    private final boolean isInUnion;
    private final List<Item> selectList;

    public FakeBaseSelectHandler(long id, Session session, List<Item> selectList, MultiNodeMergeHandler next, boolean isInUnion) {
        super(id, session);
        this.nextHandler = next;
        this.isInUnion = isInUnion;
        this.selectList = selectList;
    }

    public void fakeExecute() {
        List<FieldPacket> fields = makeFakeField();
        List<RowDataPacket> data = makeFakeRowData(fields.size());
        if (data == null || (!isInUnion && data.size() > 1)) {
            createErrorMessage();
            return;
        }
        nextHandler.fieldEofResponse(null, null, fields, null, false, null);
        for (RowDataPacket row : data) {
            nextHandler.rowResponse(null, row, false, null);
        }
        nextHandler.rowEofResponse(null, false, null);
    }

    public String toSQLString() {
        StringBuffer sb = new StringBuffer("");
        for (Item i : selectList) {
            sb.append(i.getItemName() + ";");
        }
        return sb.toString();
    }

    private List<FieldPacket> makeFakeField() {
        List<FieldPacket> totalResult = new ArrayList<>();
        for (Item i : selectList) {
            List<FieldPacket> fields = ((ItemFuncInner) i).getField();
            totalResult.addAll(fields);
        }
        return totalResult;
    }

    private List<RowDataPacket> makeFakeRowData(int fieldCount) {
        if (selectList.size() > 1) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            for (Item i : selectList) {
                List<RowDataPacket> rows = ((ItemFuncInner) i).getRows(((NonBlockingSession) session).getShardingService());
                if (rows.size() > 1) {
                    return null;
                } else {
                    row.addAll(rows.get(0).getFieldValues());
                }
            }
            List<RowDataPacket> result = new ArrayList<>();
            result.add(row);
            return result;
        } else {
            Item i = selectList.get(0);
            List<RowDataPacket> rows = ((ItemFuncInner) i).getRows(((NonBlockingSession) session).getShardingService());
            return rows;
        }
    }

    private void createErrorMessage() {
        session.onQueryError("Subquery returns more than 1 row".getBytes());
    }

    @Override
    public HandlerType type() {
        return null;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {

    }

    @Override
    protected void onTerminate() throws Exception {

    }
}
