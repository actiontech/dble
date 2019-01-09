/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.route.parser.util.ParseUtil;
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
    private Boolean readOnly = true;
    private boolean sessionReadOnly = true;

    @Override
    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public void setSessionReadOnly(boolean sessionReadOnly) {
        this.sessionReadOnly = sessionReadOnly;
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

        if (source.getSession2().getRemingSql() != null) {
            sql = source.getSession2().getRemingSql();
        }
        //Preliminary judgment of multi statement
        if (source.isMultStatementAllow() && source.getSession2().generalNextStatement(sql)) {
            sql = sql.substring(0, ParseUtil.findNextBreak(sql));
        }
        source.setExecuteSql(sql);

        int rs = ServerParse.parse(sql);
        boolean isWithHint = ServerParse.startWithHint(sql);
        int sqlType = rs & 0xff;
        if (isWithHint) {
            if (sqlType == ServerParse.INSERT || sqlType == ServerParse.DELETE || sqlType == ServerParse.UPDATE ||
                    sqlType == ServerParse.DDL) {
                if (readOnly) {
                    c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                } else if (sessionReadOnly) {
                    c.writeErrMessage(ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, "Cannot execute statement in a READ ONLY transaction.");
                }
            }
            c.execute(sql, rs & 0xff);
        } else {
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
                    LOGGER.info("Unsupported command:" + sql);
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
                case ServerParse.SCRIPT_PREPARE:
                    ScriptPrepareHandler.handle(sql, c);
                    break;
                case ServerParse.HELP:
                    LOGGER.info("Unsupported command:" + sql);
                    c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
                    break;
                case ServerParse.MYSQL_CMD_COMMENT:
                    boolean multiStatementFlag = source.getSession2().getIsMultiStatement().get();
                    c.write(c.writeToBuffer(source.getSession2().getOkByteArray(), c.allocate()));
                    c.getSession2().multiStatementNextSql(multiStatementFlag);
                    break;
                case ServerParse.MYSQL_COMMENT:
                    boolean multiStatementFlag2 = source.getSession2().getIsMultiStatement().get();
                    c.write(c.writeToBuffer(source.getSession2().getOkByteArray(), c.allocate()));
                    c.getSession2().multiStatementNextSql(multiStatementFlag2);
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
                case ServerParse.CREATE_VIEW:
                    CreateViewHandler.handle(sql, c, false);
                    break;
                case ServerParse.REPLACE_VIEW:
                    CreateViewHandler.handle(sql, c, true);
                    break;
                case ServerParse.ALTER_VIEW:
                    CreateViewHandler.handle(sql, c, false);
                    break;
                case ServerParse.DROP_VIEW:
                    DropViewHandler.handle(sql, c);
                    break;
                case ServerParse.UNSUPPORT:
                    LOGGER.info("Unsupported statement:" + sql);
                    c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported statement");
                    break;
                default:
                    if (readOnly) {
                        c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                        break;
                    } else if (sessionReadOnly) {
                        c.writeErrMessage(ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, "Cannot execute statement in a READ ONLY transaction.");
                        break;
                    }
                    c.execute(sql, rs & 0xff);
            }
        }
    }

}
