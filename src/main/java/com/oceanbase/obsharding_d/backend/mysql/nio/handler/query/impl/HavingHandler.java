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
import com.oceanbase.obsharding_d.plan.common.item.Item;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * having is just as same as where
 *
 * @author oceanbase
 */
public class HavingHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(HavingHandler.class);

    public HavingHandler(long id, Session session, Item having) {
        super(id, session);
        assert (having != null);
        this.having = having;
    }

    private Item having = null;
    private Item havingItem = null;
    private List<Field> sourceFields;
    private ReentrantLock lock = new ReentrantLock();

    @Override
    public HandlerType type() {
        return HandlerType.HAVING;
    }

    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, final List<FieldPacket> fieldPackets,
                                 byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        session.setHandlerStart(this);
        if (terminate.get())
            return;
        this.fieldPackets = fieldPackets;
        this.sourceFields = HandlerTool.createFields(this.fieldPackets);
        /**
         * having will not be pushed down because of aggregate function
         */
        this.havingItem = HandlerTool.createItem(this.having, this.sourceFields, 0, false, this.type());
        nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, final RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get())
            return true;
        lock.lock();
        try {
            HandlerTool.initFields(this.sourceFields, rowPacket.fieldValues);
            /* filter by having statement */
            if (havingItem.valBool()) {
                nextHandler.rowResponse(null, rowPacket, this.isLeft, service);
            } else {
                // nothing
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.debug("roweof");
        if (terminate.get())
            return;
        session.setHandlerEnd(this);
        nextHandler.rowEofResponse(data, this.isLeft, service);
    }

    @Override
    public void onTerminate() {
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.HAVING_FILTER;
    }
}
