/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.ChangeUserPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FrontendCommandHandler
 *
 * @author mycat
 */
public class FrontendCommandHandler implements NIOHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCommandHandler.class);
    protected final FrontendConnection source;
    protected final CommandCount commands;
    private volatile byte[] dataTodo;
    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<byte[]>();
    private AtomicBoolean isAuthSwitch = new AtomicBoolean(false);
    private volatile ChangeUserPacket changeUserPacket;

    FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        if (data.length > DbleServer.getInstance().getConfig().getSystem().getMaxPacketSize()) {
            MySQLMessage mm = new MySQLMessage(data);
            mm.readUB3();
            byte packetId = mm.read();
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setErrNo(ErrorCode.ER_NET_PACKET_TOO_LARGE);
            errPacket.setMessage("Got a packet bigger than 'max_allowed_packet' bytes.".getBytes());
            //close the mysql connection if error occur
            errPacket.setPacketId(++packetId);
            errPacket.write(source);
            return;
        }
        if (source.getLoadDataInfileHandler() != null && source.getLoadDataInfileHandler().isStartLoadData()) {
            MySQLMessage mm = new MySQLMessage(data);
            int packetLength = mm.readUB3();
            if (packetLength + 4 == data.length) {
                source.loadDataInfileData(data);
            }
            return;
        }

        if (MySQLPacket.COM_STMT_SEND_LONG_DATA == data[4]) {
            commands.doStmtSendLongData();
            blobDataQueue.offer(data);
            return;
        } else if (MySQLPacket.COM_STMT_CLOSE == data[4]) {
            commands.doStmtClose();
            source.stmtClose(data);
            return;
        } else {
            dataTodo = data;
            if (MySQLPacket.COM_STMT_RESET == data[4]) {
                blobDataQueue.clear();
            }
        }
        if (source instanceof ServerConnection) {
            ((ServerConnection) source).getSession2().resetMultiStatementStatus();
        }

        source.preparePushToQueue();
        DbleServer.getInstance().getFrontHandlerQueue().offer(this);
        source.finishPushToQueue();
    }

    public void handle() {
        try {
            handleData(dataTodo);
        } catch (Throwable e) {
            String msg = e.getMessage();
            if (StringUtil.isEmpty(msg)) {
                LOGGER.info("Maybe occur a bug, please check it.", e);
                msg = e.toString();
            } else {
                LOGGER.info("There is an error you may need know.", e);
            }
            source.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }

    protected void handleData(byte[] data) {
        source.startProcess();
        if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            source.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                source.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                source.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                source.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                source.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                source.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                source.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                source.stmtReset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                source.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                source.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                source.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                changeUserPacket = new ChangeUserPacket(source.getClientFlags(), CharsetUtil.getCollationIndex(source.getCharset().getCollation()));
                source.changeUser(data, changeUserPacket, isAuthSwitch);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                source.resetConnection();
                break;
            default:
                commands.doOther();
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
