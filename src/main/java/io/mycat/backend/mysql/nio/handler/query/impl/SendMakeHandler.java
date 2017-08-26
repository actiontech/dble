package io.mycat.backend.mysql.nio.handler.query.impl;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.query.BaseDMLHandler;
import io.mycat.backend.mysql.nio.handler.util.HandlerTool;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.server.NonBlockingSession;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * if the Item is Item_sum,then theItem must be generated in GroupBy.Otherwise,calc by middle-ware
 */
public class SendMakeHandler extends BaseDMLHandler {

    private final ReentrantLock lock;

    private List<Item> sels;
    private List<Field> sourceFields;
    private List<Item> selItems;
    private String tbAlias;

    /**
     * @param session
     * @param sels
     */
    public SendMakeHandler(long id, NonBlockingSession session, List<Item> sels, String tableAlias) {
        super(id, session);
        lock = new ReentrantLock();
        this.sels = sels;
        this.selItems = new ArrayList<>();
        this.tbAlias = tableAlias;
    }

    @Override
    public HandlerType type() {
        return HandlerType.SENDMAKER;
    }

    @Override
    public void fieldEofResponse(byte[] headernull, List<byte[]> fieldsnull, List<FieldPacket> fieldPackets,
                                 byte[] eofnull, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            this.fieldPackets = fieldPackets;
            this.sourceFields = HandlerTool.createFields(this.fieldPackets);
            for (Item sel : sels) {
                Item tmpItem = HandlerTool.createItem(sel, this.sourceFields, 0, isAllPushDown(), type(),
                        conn.getCharset());
                tmpItem.setItemName(sel.getItemName());
                if (sel.getAlias() != null || tbAlias != null) {
                    String selAlias = sel.getAlias();
                    // remove the added tmp FNAF
                    if (StringUtils.indexOf(selAlias, Item.FNAF) == 0)
                        selAlias = StringUtils.substring(selAlias, Item.FNAF.length());
                    tmpItem = HandlerTool.createRefItem(tmpItem, tbAlias, selAlias);
                }
                this.selItems.add(tmpItem);
            }
            List<FieldPacket> newFieldPackets = new ArrayList<>();
            for (Item selItem : this.selItems) {
                FieldPacket tmpFp = new FieldPacket();
                selItem.makeField(tmpFp);
                /* Keep things compatible for old clients */
                if (tmpFp.getType() == FieldTypes.MYSQL_TYPE_VARCHAR.numberValue())
                    tmpFp.setType(FieldTypes.MYSQL_TYPE_VAR_STRING.numberValue());
                newFieldPackets.add(tmpFp);
            }
            nextHandler.fieldEofResponse(null, null, newFieldPackets, null, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return true;
            HandlerTool.initFields(sourceFields, rowPacket.fieldValues);
            RowDataPacket newRp = new RowDataPacket(selItems.size());
            for (Item selItem : selItems) {
                byte[] b = selItem.getRowPacketByte();
                newRp.add(b);
            }
            nextHandler.rowResponse(null, newRp, this.isLeft, conn);
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        lock.lock();
        try {
            if (terminate.get())
                return;
            nextHandler.rowEofResponse(eof, this.isLeft, conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onTerminate() {
    }


}
