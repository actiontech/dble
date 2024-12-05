/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;


import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class RenameFieldHandler extends BaseDMLHandler {
    private String alias;
    private PlanNode.PlanNodeType childType;

    public RenameFieldHandler(long id, Session session, String alias, PlanNode.PlanNodeType childType) {
        super(id, session);
        this.alias = alias;
        this.childType = childType;
    }

    @Override
    public HandlerType type() {
        return HandlerType.RENAME_FIELD;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        for (FieldPacket fp : fieldPackets) {
            fp.setTable(alias.getBytes());
            if (childType.equals(PlanNode.PlanNodeType.TABLE)) {
                //mysql 5.6 Org Table is child's name, 5.7 is *
                fp.setOrgTable("*".getBytes());
            }
        }
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        nextHandler.rowResponse(rowNull, rowPacket, this.isLeft, service);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(eof, this.isLeft, service);
    }

    @Override
    protected void onTerminate() throws Exception {
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.RENAME_DERIVED_SUB_QUERY;
    }
}
