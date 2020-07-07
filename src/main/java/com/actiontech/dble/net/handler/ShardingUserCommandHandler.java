/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ChangeUserPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.ServerConnection;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShardingUserCommandHandler extends FrontendCommandHandler {
    private volatile ChangeUserPacket changeUserPacket;

    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<byte[]>();
    private AtomicBoolean isAuthSwitch = new AtomicBoolean(false);

    ShardingUserCommandHandler(ServerConnection source) {
        super(source);
    }

    protected void handleDataByPacket(byte[] data) {
        ServerConnection sc = (ServerConnection) source;
        if (sc.getLoadDataInfileHandler() != null && sc.getLoadDataInfileHandler().isStartLoadData()) {
            MySQLMessage mm = new MySQLMessage(data);
            int packetLength = mm.readUB3();
            if (packetLength + 4 == data.length) {
                sc.loadDataInfileData(data);
            }
            return;
        }

        if (MySQLPacket.COM_STMT_SEND_LONG_DATA == data[4]) {
            commands.doStmtSendLongData();
            blobDataQueue.offer(data);
            return;
        } else if (MySQLPacket.COM_STMT_CLOSE == data[4]) {
            commands.doStmtClose();
            sc.stmtClose(data);
            return;
        } else {
            dataTodo = data;
            if (MySQLPacket.COM_STMT_RESET == data[4]) {
                blobDataQueue.clear();
            }
        }
        sc.getSession2().resetMultiStatementStatus();

        DbleServer.getInstance().getFrontHandlerQueue().offer(this);
    }

    protected void handleData(byte[] data) {
        ServerConnection sc = (ServerConnection) source;
        sc.startProcess();

        if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            sc.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                sc.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                sc.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                sc.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                sc.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                sc.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                sc.stmtPrepare(data);
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                sc.stmtReset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                sc.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                sc.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                sc.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                commands.doOther();
                changeUserPacket = new ChangeUserPacket(sc.getClientFlags(), CharsetUtil.getCollationIndex(sc.getCharset().getCollation()));
                sc.changeUser(data, changeUserPacket, isAuthSwitch);
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                sc.resetConnection();
                break;
            case MySQLPacket.COM_FIELD_LIST:
                commands.doOther();
                sc.fieldList(data);
                break;
            default:
                commands.doOther();
                sc.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
