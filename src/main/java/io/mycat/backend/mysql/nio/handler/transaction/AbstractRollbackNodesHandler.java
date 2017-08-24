package io.mycat.backend.mysql.nio.handler.transaction;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.MultiNodeHandler;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractRollbackNodesHandler extends MultiNodeHandler implements RollbackNodesHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractRollbackNodesHandler.class);

    public AbstractRollbackNodesHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.error("unexpected invocation: connectionAcquired from rollback");
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

}
