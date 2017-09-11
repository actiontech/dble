/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * union all statement handler
 * and union statement split to union all handler and distinctHandler
 *
 * @author ActionTech
 */
public class UnionHandler extends BaseDMLHandler {
    private static final Logger LOGGER = Logger.getLogger(UnionHandler.class);

    public UnionHandler(long id, NonBlockingSession session, List<Item> sels, int nodecount) {
        super(id, session);
        this.sels = sels;
        this.nodeCount = new AtomicInteger(nodecount);
        this.nodeCountField = new AtomicInteger(nodecount);
    }

    /**
     * union may has multi tables,but the result's columns are same as the first table's
     */
    private List<Item> sels;
    private AtomicInteger nodeCount;
    /* used for fieldeof */
    private AtomicInteger nodeCountField;
    private ReentrantLock lock = new ReentrantLock();
    private Condition conFieldSend = lock.newCondition();

    @Override
    public HandlerType type() {
        return HandlerType.UNION;
    }

    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, final List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        lock.lock();
        try {
            if (this.fieldPackets == null || this.fieldPackets.size() == 0) {
                this.fieldPackets = fieldPackets;
            } else {
                this.fieldPackets = unionFieldPackets(this.fieldPackets, fieldPackets);
            }
            if (nodeCountField.decrementAndGet() == 0) {
                // set correct name to fieldpackets
                checkFieldPackets();
                nextHandler.fieldEofResponse(null, null, this.fieldPackets, null, this.isLeft, conn);
                conFieldSend.signalAll();
            } else {
                while (nodeCountField.get() != 0) {
                    conFieldSend.await();
                }
            }
        } catch (Exception e) {
            String msg = "Union field merge error, " + e.getLocalizedMessage();
            LOGGER.warn(msg, e);
            conFieldSend.signalAll();
            session.onQueryError(msg.getBytes());
        } finally {
            lock.unlock();
        }
    }

    private void checkFieldPackets() {
        for (int i = 0; i < sels.size(); i++) {
            FieldPacket fp = this.fieldPackets.get(i);
            Item sel = sels.get(i);
            fp.setName(sel.getItemName().getBytes());
            fp.setTable(sel.getTableName().getBytes());
        }
    }

    /**
     * merge fieldpakcets with fieldpackets2
     * eg: int field union double field ->double field
     *
     * @param fieldPackets
     * @param fieldPackets2
     */
    private List<FieldPacket> unionFieldPackets(List<FieldPacket> fieldPackets, List<FieldPacket> fieldPackets2) {
        List<FieldPacket> newFps = new ArrayList<>();
        for (int i = 0; i < fieldPackets.size(); i++) {
            FieldPacket fp1 = fieldPackets.get(i);
            FieldPacket fp2 = fieldPackets2.get(i);
            FieldPacket newFp = unionFieldPacket(fp1, fp2);
            newFps.add(newFp);
        }
        return newFps;
    }

    private FieldPacket unionFieldPacket(FieldPacket fp1, FieldPacket fp2) {
        FieldPacket union = new FieldPacket();
        union.setCatalog(fp1.getCatalog());
        union.setCharsetIndex(fp1.getCharsetIndex());
        union.setDb(fp1.getDb());
        union.setDecimals((byte) Math.max(fp1.getDecimals(), fp2.getDecimals()));
        union.setDefinition(fp1.getDefinition());
        union.setFlags(fp1.getFlags() | fp2.getFlags());
        union.setLength(Math.max(fp1.getLength(), fp2.getLength()));
        FieldTypes fieldType1 = FieldTypes.valueOf(fp1.getType());
        FieldTypes fieldType2 = FieldTypes.valueOf(fp2.getType());
        FieldTypes mergeFieldType = FieldUtil.fieldTypeMerge(fieldType1, fieldType2);
        union.setType(mergeFieldType.numberValue());
        return union;
    }

    /**
     * need wait for all field merged
     */
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
        return false;
    }

    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return;
        if (nodeCount.decrementAndGet() == 0) {
            nextHandler.rowEofResponse(data, this.isLeft, conn);
        }
    }

    @Override
    public void onTerminate() {
        lock.lock();
        try {
            this.conFieldSend.signalAll();
        } finally {
            lock.unlock();
        }
    }

}
