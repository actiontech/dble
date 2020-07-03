/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.CallBackHandler;
import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SubQueryHandler extends BaseDMLHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SubQueryHandler.class);
    protected final ReentrantLock lock;
    protected CallBackHandler tempDoneCallBack;
    protected ErrorPacket errorPacket;

    public abstract void setForExplain();

    public SubQueryHandler(long id, Session session) {
        super(id, session);
        this.lock = new ReentrantLock();
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        lock.lock();
        try {
            // callBack after terminated
            if (terminate.get()) {
                return;
            }
            session.setHandlerEnd(this);
            HandlerTool.terminateHandlerTree(this);
            // locked onTerminate, because terminated may sync with start
            tempDoneCallBack.call();
        } catch (Exception callback) {
            LOGGER.info("callback exception!", callback);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        lock.lock();
        try {
            String errorMsg = e.getMessage() == null ? e.toString() : e.getMessage();
            LOGGER.info(errorMsg);
            genErrorPackage(ErrorCode.ER_UNKNOWN_ERROR, errorMsg);
            HandlerTool.terminateHandlerTree(this);
            tempDoneCallBack.call();
        } catch (Exception callback) {
            LOGGER.info("callback exception!", callback);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void onTerminate() {
        //do nothing
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        lock.lock();
        try {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.read(err);
            String errorMsg = new String(errPacket.getMessage(), StandardCharsets.UTF_8);
            LOGGER.info(errorMsg);
            genErrorPackage(errPacket.getErrNo(), errorMsg);
            HandlerTool.terminateHandlerTree(this);
            tempDoneCallBack.call();
        } catch (Exception callback) {
            LOGGER.info("callback exception!", callback);
        } finally {
            lock.unlock();
        }
    }

    protected void genErrorPackage(int errorNum, String msg) {
        if (errorPacket == null) {
            errorPacket = new ErrorPacket();
            errorPacket.setErrNo(errorNum);
            errorPacket.setMessage(msg.getBytes(StandardCharsets.UTF_8));
        }
    }

    public void setTempDoneCallBack(CallBackHandler tempDoneCallBack) {
        this.tempDoneCallBack = tempDoneCallBack;
    }

    public ErrorPacket getErrorPacket() {
        return errorPacket;
    }
}
