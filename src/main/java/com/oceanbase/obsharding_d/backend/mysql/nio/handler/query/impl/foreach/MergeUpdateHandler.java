/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.foreach;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.MultiNodeUpdateHandler;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;

import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MergeUpdateHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeUpdateHandler.class);
    private final long row;
    private long rowCount = 0;
    private long affectedRows;
    protected final ReentrantLock lock;

    public MergeUpdateHandler(long id, Session session, long row) {
        super(id, session);
        this.row = row;
        this.lock = new ReentrantLock();
    }


    @Override
    protected void onTerminate() {

    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        if (this.terminate.get())
            return;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,from " + service);
        }

        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        lock.lock();
        try {
            affectedRows += okPacket.getAffectedRows();
            if (++rowCount != row) {
                return;
            }

            okPacket.setAffectedRows(affectedRows);
            ((MySQLResponseService) service).getSession().setRowCount(affectedRows);
            okPacket.setServerStatus(((MySQLResponseService) service).getSession().getShardingService().isAutocommit() ? 2 : 1);
            okPacket.setMessage(null);
            nextHandler.okResponse(okPacket.toBytes(), service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @Nonnull AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @Nonnull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @Nonnull AbstractService service) {

    }

    @Override
    public HandlerType type() {
        return HandlerType.MERGE_UPDATE;
    }

    @Override
    public ExplainType explainType() {
        return ExplainType.MERGE_UPDATE;
    }
}
