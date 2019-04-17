/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<byte[]>();

    public FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        if (source.getLoadDataInfileHandler() != null && source.getLoadDataInfileHandler().isStartLoadData()) {
            MySQLMessage mm = new MySQLMessage(data);
            int packetLength = mm.readUB3();
            if (packetLength + 4 == data.length) {
                source.loadDataInfileData(data);
            }
            return;
        }

        if (MySQLPacket.COM_STMT_SEND_LONG_DATA == data[4]) {
            blobDataQueue.offer(data);
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
        DbleServer.getInstance().getFrontHandlerQueue().offer(this);
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
            case MySQLPacket.COM_STMT_CLOSE:
                commands.doStmtClose();
                source.stmtClose(data);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                source.heartbeat(data);
                break;
            default:
                commands.doOther();
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
