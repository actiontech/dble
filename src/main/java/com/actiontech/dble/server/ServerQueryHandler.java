/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.handler.*;
import com.actiontech.dble.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class ServerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final ServerConnection source;
    protected Boolean readOnly = true;

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public ServerQueryHandler(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void query(String sql) {

        ServerConnection c = this.source;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(c) + sql);
        }
        //
        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        switch (sqlType) {
            //explain sql
            case ServerParse.EXPLAIN:
                ExplainHandler.handle(sql, c, rs >>> 8);
                break;
            //explain2 datanode=? sql=?
            case ServerParse.EXPLAIN2:
                Explain2Handler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.DESCRIBE:
                DescribeHandler.handle(sql, c);
                break;
            case ServerParse.SET:
                SetHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.START:
                StartHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.BEGIN:
                BeginHandler.handle(sql, c);
                break;
            case ServerParse.SAVEPOINT:
                SavepointHandler.handle(sql, c);
                break;
            case ServerParse.KILL:
                KillHandler.handle(sql, rs >>> 8, c);
                break;
            case ServerParse.KILL_QUERY:
                LOGGER.warn("Unsupported command:" + sql);
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.COMMIT:
                CommitHandler.handle(sql, c);
                break;
            case ServerParse.ROLLBACK:
                RollBackHandler.handle(sql, c);
                break;
            case ServerParse.HELP:
                LOGGER.warn("Unsupported command:" + sql);
                c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
                break;
            case ServerParse.MYSQL_CMD_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.MYSQL_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.LOAD_DATA_INFILE_SQL:
                c.loadDataInfileStart(sql);
                break;
            case ServerParse.LOCK:
                c.lockTable(sql);
                break;
            case ServerParse.UNLOCK:
                c.unLockTable(sql);
                break;
            default:
                if (readOnly) {
                    LOGGER.warn("User readonly:" + sql);
                    c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
                    break;
                }
                c.execute(sql, rs & 0xff);
        }
    }

}
