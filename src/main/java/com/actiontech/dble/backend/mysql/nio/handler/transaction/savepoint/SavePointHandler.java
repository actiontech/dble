package com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.actiontech.dble.config.ErrorCode.ER_SP_DOES_NOT_EXIST;

public class SavePointHandler extends MultiNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SavePointHandler.class);
    private static final SavePoint TAIL = new SavePoint(null);
    public enum Type {
        SAVE, ROLLBACK, RELEASE
    }
    private byte[] sendData = OkPacket.OK;
    private SavePoint savepoints = TAIL;

    public SavePointHandler(NonBlockingSession session) {
        super(session);
    }

    public void perform(String spName, Type type) {
        switch (type) {
            case SAVE:
                save(spName);
                break;
            case ROLLBACK:
                rollbackTo(spName);
                break;
            case RELEASE:
                release(spName);
                break;
            default:
                LOGGER.warn("unknown savepoint perform type!");
                break;
        }
    }

    private void save(String spName) {
        SavePoint newSp = new SavePoint(spName);

        if (savepoints == null || session.getTargetCount() <= 0) {
            newSp.setRouteNodes(session.getTargetKeys());
            addSavePoint(newSp);
            session.getSource().write(OkPacket.OK);
            return;
        }

        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }

        Set<RouteResultsetNode> newNodes = null;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            if (!savepoints.getRouteNodes().contains(rrn)) {
                if (newNodes == null) {
                    newNodes = new HashSet<>(session.getTargetKeys());
                }
                newNodes.add(rrn);
            }
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            ((MySQLConnection) conn).execCmd("savepoint " + spName);
        }

        if (newNodes != null) {
            newSp.setRouteNodes(newNodes);
            addSavePoint(newSp);
        } else {
            newSp.setRouteNodes(savepoints.getRouteNodes());
            addSavePoint(newSp);
        }
    }

    private void rollbackTo(String spName) {
        SavePoint sp = findSavePoint(spName);
        if (sp == null || sp.getPrev() == null) {
            session.getSource().writeErrMessage(ER_SP_DOES_NOT_EXIST, "SAVEPOINT " + spName + " does not exist");
            return;
        }

        if (session.getTargetCount() <= 0) {
            savepoints = sp.getPrev();
            sp.setPrev(null);
            session.getSource().write(OkPacket.OK);
            return;
        }

        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }

        Set lastNodes = sp.getPrev().getRouteNodes();
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            if (!lastNodes.contains(rrn)) {
                // rollback connection
                ((MySQLConnection) conn).execCmd("rollback");
            } else {
                // rollback to
                ((MySQLConnection) conn).execCmd("rollback to " + spName);
            }
        }
        savepoints = sp.getPrev();
        sp.setPrev(null);
    }

    private void release(String spName) {
        SavePoint sp = findSavePoint(spName);
        if (sp == null || sp.getPrev() == null) {
            session.getSource().writeErrMessage(ER_SP_DOES_NOT_EXIST, "SAVEPOINT " + spName + " does not exist");
            return;
        }
        sp = sp.getPrev();
        if (session.getTargetCount() > 0) {
            savepoints = sp.getPrev();
            sp.setPrev(null);
        }
        session.getSource().write(OkPacket.OK);
    }

    // find savepoint after named savepoint
    private SavePoint findSavePoint(String name) {
        SavePoint latter = null;
        SavePoint sp = savepoints;
        while (sp != null) {
            if (name.equals(sp.getName())) {
                break;
            }
            latter = sp;
            sp = sp.getPrev();
        }
        return latter;
    }

    private void addSavePoint(SavePoint newSp) {
        SavePoint sp = findSavePoint(newSp.getName());
        // removed named savepoint
        if (sp != null && sp.getPrev() != null) {
            sp.getPrev().setPrev(null);
            sp.setPrev(sp.getPrev().getPrev());
        }
        newSp.setPrev(savepoints);
        savepoints = newSp;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        LOGGER.warn("get error package, content is:" + errMsg);
        this.setFail(errMsg);
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(final BackendConnection conn, final String reason) {
        LOGGER.warn("backend connection closed:" + reason + ", conn info:" + conn);
        String errMsg = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        this.setFail(errMsg);
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("backend connect err:", e);
        this.setFail(e.getMessage());
        conn.close("savepoint connection Error");
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    private void cleanAndFeedback() {
        byte[] send = sendData;
        // clear all resources
        if (session.closed()) {
            return;
        }
        if (this.isFail()) {
            createErrPkg(error).write(session.getSource());
        } else {
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(send);
            session.multiStatementNextSql(multiStatementFlag);
        }
    }

    @Override
    public void clearResources() {
        TAIL.setPrev(null);
        savepoints = TAIL;
    }

    @Override
    public void writeQueueAvailable() {
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected filed eof response in savepoint");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected row response in savepoint");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected row eof response in savepoint");
    }

}
