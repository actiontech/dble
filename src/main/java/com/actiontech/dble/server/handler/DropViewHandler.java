/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.util.ArrayList;
import java.util.List;

import static com.actiontech.dble.config.ErrorCode.ER_BAD_TABLE_ERROR;
import static com.actiontech.dble.config.ErrorCode.ER_PARSE_ERROR;

/**
 * Created by szf on 2017/10/17.
 */
public final class DropViewHandler {

    private DropViewHandler() {

    }

    public static void handle(String stmt, ServerConnection c) {
        try {
            SQLStatementParser parser;
            parser = new MySqlStatementParser(stmt);
            SQLDropViewStatement viewStatement;
            try {
                viewStatement = (SQLDropViewStatement) parser.parseStatement(true);
            } catch (Exception t) {
                c.writeErrMessage(ER_BAD_TABLE_ERROR, " view drop sql error");
                return;
            }

            boolean ifExistsflag = viewStatement.isIfExists();
            List<SQLExprTableSource> removeList = new ArrayList<>();
            if (viewStatement.getTableSources() != null && viewStatement.getTableSources().size() != 0) {
                for (SQLExprTableSource table : viewStatement.getTableSources()) {
                    //check table meta & add the table into list
                    String result = addToRemoveList(removeList, table, c.getSchema(), ifExistsflag);
                    if (result != null) {
                        c.writeErrMessage(ER_BAD_TABLE_ERROR, result);
                        return;
                    }
                }
            } else {
                c.writeErrMessage(ER_BAD_TABLE_ERROR, " no view in sql when try to drop");
                return;
            }

            //drop all the view
            for (SQLExprTableSource table : removeList) {
                String tableName = StringUtil.removeBackQuote(table.getName().getSimpleName()).trim();
                DbleServer.getInstance().getTmManager().addMetaLock(table.getSchema(), tableName, stmt);
                try {
                    deleteFromReposoitory(table.getSchema(), tableName);
                    DbleServer.getInstance().getTmManager().getCatalogs().get(table.getSchema()).getViewMetas().remove(tableName);
                } catch (Throwable e) {
                    throw e;
                } finally {
                    DbleServer.getInstance().getTmManager().removeMetaLock(table.getSchema(), tableName);
                }
            }

            byte packetId = (byte) c.getSession2().getPacketId().get();
            OkPacket ok = new OkPacket();
            ok.setPacketId(++packetId);
            c.getSession2().multiStatementPacket(ok, packetId);
            ok.write(c);
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            c.getSession2().multiStatementNextSql(multiStatementFlag);
            return;
        } catch (Throwable e) {
            c.writeErrMessage(ER_PARSE_ERROR, "Get Error when delete the view");
        }
    }

    public static void deleteFromReposoitory(String schema, String name) {
        DbleServer.getInstance().getTmManager().getRepository().delete(schema, name);
    }


    private static String addToRemoveList(List<SQLExprTableSource> removeList, SQLExprTableSource table, String defaultSchema, boolean ifExistsflag) {
        if (table.getSchema() != null) {
            table.setSchema(StringUtil.removeBackQuote(table.getSchema()));
        } else {
            table.setSchema(defaultSchema);
        }

        if (!(DbleServer.getInstance().getTmManager().getCatalogs().get(table.getSchema()).
                getViewMetas().containsKey(StringUtil.removeBackQuote(table.getName().getSimpleName())))) {
            if (!ifExistsflag) {
                return " Unknown table '" + table.getName().toString() + "'";
            }
        } else {
            removeList.add(table);
        }
        return null;
    }
}
