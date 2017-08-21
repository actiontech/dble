package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.backend.mysql.nio.handler.util.CallBackHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.backend.mysql.store.UnSortedLocalResult;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.exception.TempTableException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.meta.TempTable;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 将结果集生成到临时表中
 */
public class TempTableHandler extends BaseDMLHandler {
    private static final Logger logger = Logger.getLogger(TempTableHandler.class);

    private final ReentrantLock lock;
    private final TempTable tempTable;

    private int maxPartSize = 2000;
    private int maxConnSize = 4;
    private int rowCount = 0;
    private CallBackHandler tempDoneCallBack;
    // 由tempHandler生成的Handler，还得由它来释放
    private DMLResponseHandler createdHandler;

    private int sourceSelIndex = -1;
    private final Item sourceSel;
    private Field sourceField;
    private Set<String> valueSet;

    public TempTableHandler(long id, NonBlockingSession session, Item sourceSel) {
        super(id, session);
        this.lock = new ReentrantLock();
        this.tempTable = new TempTable();
        this.maxPartSize = MycatServer.getInstance().getConfig().getSystem().getNestLoopRowsSize();
        this.maxConnSize = MycatServer.getInstance().getConfig().getSystem().getNestLoopConnSize();
        this.sourceSel = sourceSel;
        this.valueSet = new HashSet<String>();
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        if (terminate.get()) {
            return;
        }
        lock.lock();
        try {
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                tempTable.setFieldPackets(this.fieldPackets);
                tempTable.setCharset(conn.getCharset());
                tempTable.setRowsStore(new UnSortedLocalResult(fieldPackets.size(), MycatServer.getInstance().getBufferPool(),
                        conn.getCharset()).setMemSizeController(session.getOtherBufferMC()));
                List<Field> fields = HandlerTool.createFields(this.fieldPackets);
                sourceSelIndex = HandlerTool.findField(sourceSel, fields, 0);
                if (sourceSelIndex < 0)
                    throw new TempTableException("sourcesel [" + sourceSel.toString() + "] not found in fields");
                sourceField = fields.get(sourceSelIndex);
                if (nextHandler != null) {
                    nextHandler.fieldEofResponse(headernull, fieldsnull, fieldPackets, eofnull, this.isLeft, conn);
                } else {
                    throw new TempTableException("unexpected nextHandler is null");
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get()) {
                return true;
            }
            if (++rowCount > maxPartSize * maxConnSize) {
                String errMessage = "temptable of [" + conn.toString() + "] too much rows,[rows=" + rowCount + "]!";
                logger.warn(errMessage);
                throw new TempTableException(errMessage);
            }
            RowDataPacket row = rowPacket;
            if (row == null) {
                row = new RowDataPacket(this.fieldPackets.size());
                row.read(rownull);
            }
            tempTable.addRow(row);
            sourceField.setPtr(row.getValue(sourceSelIndex));
            valueSet.add(sourceField.valStr());
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            // terminate之后仍然进行callBack操作
            if (terminate.get()) {
                return;
            }
            tempTable.dataEof();
            // onTerminate加了锁，避免了terminate的时候启动了
            tempDoneCallBack.call();
            RowDataPacket rp = null;
            while ((rp = tempTable.nextRow()) != null) {
                nextHandler.rowResponse(null, rp, this.isLeft, conn);
            }
            nextHandler.rowEofResponse(eof, this.isLeft, conn);
        } catch (Exception e) {
            logger.warn("rowEof exception!", e);
            throw new TempTableException("rowEof exception!", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void onTerminate() {
        lock.lock();
        try {
            this.tempTable.close();
            this.valueSet.clear();
            if (createdHandler != null) {
                HandlerTool.terminateHandlerTree(createdHandler);
            }
        } finally {
            lock.unlock();
        }
    }

    public TempTable getTempTable() {
        return tempTable;
    }

    public void setTempDoneCallBack(CallBackHandler tempDoneCallBack) {
        this.tempDoneCallBack = tempDoneCallBack;
    }

    public void setCreatedHandler(DMLResponseHandler createdHandler) {
        this.createdHandler = createdHandler;
    }

    public Set<String> getValueSet() {
        return valueSet;
    }

    public int getMaxPartSize() {
        return maxPartSize;
    }

    @Override
    public HandlerType type() {
        return HandlerType.TEMPTABLE;
    }

}
