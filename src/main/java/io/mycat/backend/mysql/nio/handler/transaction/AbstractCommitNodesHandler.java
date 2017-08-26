package io.mycat.backend.mysql.nio.handler.transaction;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.MultiNodeHandler;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractCommitNodesHandler extends MultiNodeHandler implements CommitNodesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommitNodesHandler.class);

    public AbstractCommitNodesHandler(NonBlockingSession session) {
        super(session);
    }

    protected abstract boolean executeCommit(MySQLConnection mysqlCon, int position);

    @Override
    public void commit() {
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        int position = 0;
        //get session's lock before sending commit(in fact, after ended)
        //then the XA transaction will be not killed, if killed ,then we will not commit
        if (session.getXaState() != null && session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }

        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            if (!executeCommit((MySQLConnection) conn, position++)) {
                break;
            }
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " + conn +
                " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.error("unexpected invocation: connectionAcquired from commit");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
        return false;
    }

    @Override
    public void writeQueueAvailable() {

    }


    public void debugCommitDelay() {

    }
}
