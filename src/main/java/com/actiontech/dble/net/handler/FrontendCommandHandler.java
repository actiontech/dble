/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FrontendCommandHandler implements NIOHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendCommandHandler.class);
    protected final FrontendConnection source;
    protected final CommandCount commands;
    volatile byte[] dataTodo;

    FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    @Override
    public void handle(byte[] data) {
        if (data.length - MySQLPacket.PACKET_HEADER_SIZE >= SystemConfig.getInstance().getMaxPacketSize()) {
            MySQLMessage mm = new MySQLMessage(data);
            mm.readUB3();
            byte packetId = 0;
            if (source instanceof ServerConnection) {
                NonBlockingSession session = ((ServerConnection) source).getSession2();
                if (session != null) {
                    packetId = (byte) session.getPacketId().get();
                }
            } else {
                packetId = mm.read();
            }
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setErrNo(ErrorCode.ER_NET_PACKET_TOO_LARGE);
            errPacket.setMessage("Got a packet bigger than 'max_allowed_packet' bytes.".getBytes());
            //close the mysql connection if error occur
            errPacket.setPacketId(++packetId);
            errPacket.write(source);
            return;
        }
        handleDataByPacket(data);
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
    protected abstract void handleDataByPacket(byte[] data);
    protected abstract void handleData(byte[] data);

}
