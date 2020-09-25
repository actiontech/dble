package com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint;

import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.actiontech.dble.config.ErrorCode.ER_SP_DOES_NOT_EXIST;

public class SavePointHandler extends MultiNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SavePointHandler.class);
    private final SavePoint savepoints = new SavePoint(null);
    private volatile SavePoint performSp = null;
    private Type type = Type.SAVE;

    public enum Type {
        SAVE, ROLLBACK, RELEASE
    }

    private byte[] sendData = OkPacket.OK;

    public SavePointHandler(NonBlockingSession session) {
        super(session);
    }

    public void perform(String spName, Type typ) {
        this.type = typ;
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
        if (session.getTargetCount() <= 0) {
            addSavePoint(newSp);
            session.getShardingService().writeOkPacket();
            return;
        }

        lock.lock();
        try {
            reset();
        } finally {
            lock.unlock();
        }

        SavePoint latestSp = savepoints.getPrev();
        if (latestSp == null || session.getTargetCount() > latestSp.getRouteNodes().size()) {
            newSp.setRouteNodes(new HashSet<>(session.getTargetKeys()));
        } else {
            newSp.setRouteNodes(latestSp.getRouteNodes());
        }

        unResponseRrns.addAll(session.getTargetKeys());
        this.performSp = newSp;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);
            conn.getBackendService().execCmd("savepoint " + spName);
        }
    }

    private void rollbackTo(String spName) {
        SavePoint sp = findSavePoint(spName);
        if (sp == null || sp.getPrev() == null) {
            session.getShardingService().writeErrMessage(ER_SP_DOES_NOT_EXIST, "SAVEPOINT " + spName + " in dble does not exist");
            return;
        }

        if (session.getTargetCount() <= 0) {
            rollbackToSavepoint(sp);
            session.getShardingService().writeOkPacket();
            return;
        }

        lock.lock();
        try {
            reset();
        } finally {
            lock.unlock();
        }

        Set lastNodes = sp.getPrev().getRouteNodes();
        unResponseRrns.addAll(session.getTargetKeys());
        this.performSp = sp;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);
            if (!lastNodes.contains(rrn)) {
                // rollback connection
                conn.getBackendService().execCmd("rollback");
            } else {
                // rollback to
                conn.getBackendService().execCmd("rollback to " + spName);
            }
        }
    }

    private void release(String spName) {
        SavePoint sp = findSavePoint(spName);
        if (sp == null || sp.getPrev() == null) {
            session.getShardingService().writeErrMessage(ER_SP_DOES_NOT_EXIST, "SAVEPOINT " + spName + " in dble does not exist");
            return;
        }
        sp = sp.getPrev();
        if (session.getTargetCount() > 0) {
            savepoints.setPrev(sp.getPrev());
            sp.setPrev(null);
        }
        session.getShardingService().writeOkPacket();
    }

    // find savepoint after named savepoint
    private SavePoint findSavePoint(String name) {
        SavePoint latter = savepoints;
        SavePoint sp = latter.getPrev();
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
            SavePoint temp = sp.getPrev();
            sp.setPrev(temp.getPrev());
            temp.setPrev(null);
        }
        newSp.setPrev(savepoints.getPrev());
        savepoints.setPrev(newSp);
    }

    private void rollbackToSavepoint(SavePoint rollbackTo) {
        savepoints.setPrev(rollbackTo.getPrev());
        // except head savepoint that name is only null
        if (rollbackTo.getName() != null) {
            rollbackTo.setPrev(null);
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (decrementToZero((MySQLResponseService) service)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        LOGGER.warn("get error package, content is:" + errMsg);
        this.setFail(errMsg);
        if (decrementToZero((MySQLResponseService) service)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(final AbstractService service, final String reason) {
        LOGGER.warn("backend connection closed:" + reason + ", conn info:" + service);
        String errMsg = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        this.setFail(errMsg);
        RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
        session.getTargetMap().remove(rNode);
        ((MySQLResponseService) service).setResponseHandler(null);
        if (decrementToZero((MySQLResponseService) service)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("connection Error in savePointHandler, err:", e);
        boolean finished;
        lock.lock();
        try {
            errorConnsCnt++;
            finished = canResponse();
        } finally {
            lock.unlock();
        }
        if (finished) {
            cleanAndFeedback();
        }
    }

    private void cleanAndFeedback() {
        // clear all resources
        if (session.closed()) {
            return;
        }
        if (this.isFail()) {
            createErrPkg(error, 0).write(session.getSource());
        } else {
            switch (this.type) {
                case SAVE:
                    addSavePoint(performSp);
                    break;
                case ROLLBACK:
                    rollbackToSavepoint(performSp);
                    break;
                default:
                    LOGGER.warn("unknown savepoint perform type!");
                    break;
            }
            OkPacket ok = new OkPacket().read(sendData);
            ok.write(session.getSource());
        }
    }

    @Override
    public void clearResources() {
        savepoints.setPrev(null);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        LOGGER.warn("unexpected filed eof response in savepoint");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        LOGGER.warn("unexpected row response in savepoint");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        LOGGER.warn("unexpected row eof response in savepoint");
    }

}
