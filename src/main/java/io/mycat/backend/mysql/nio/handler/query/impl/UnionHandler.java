package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.FieldUtil;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * union all语句的handler，如果是union语句的话，则在handlerbuilder时，
 * 向unionallhandler后面添加distinctHandler
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
     * 因为union有可能是多个表，最终出去的节点仅按照第一个表的列名来
     */
    private List<Item> sels;
    private AtomicInteger nodeCount;
    /* 供fieldeof使用的 */
    private AtomicInteger nodeCountField;
    private ReentrantLock lock = new ReentrantLock();
    private Condition conFieldSend = lock.newCondition();

    @Override
    public HandlerType type() {
        return HandlerType.UNION;
    }

    /**
     * 所有的上一级表传递过来的信息全部视作Field类型
     */
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
                // 将fieldpackets赋成正确的fieldname
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
     * 将fieldpakcets和fieldpackets2进行merge，比如说
     * 一个int的列和一个double的列union完了之后结果是一个double的列
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
     * 收到行数据包的响应处理，这里需要等上面的field都merge完了才可以发送
     */
    public boolean rowResponse(byte[] rownull, final RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
        return false;
    }

    /**
     * 收到行数据包结束的响应处理
     */
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
