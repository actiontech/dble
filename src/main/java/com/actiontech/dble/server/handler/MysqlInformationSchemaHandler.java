/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.UserConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * MysqlInformationSchemaHandler
 * <p>
 * :SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA
 *
 * @author zhuam
 */
public final class MysqlInformationSchemaHandler {
    private MysqlInformationSchemaHandler() {
    }

    /**
     * @param schemaInfo
     * @param c
     */
    public static void handle(SchemaInfo schemaInfo, ServerConnection c) {
        if (schemaInfo != null) {
            String tableName = schemaInfo.getTable();
            if (MysqlSystemSchemaHandler.SCHEMATA_TABLE.equalsIgnoreCase(tableName)) {
                int fieldCount = 3;
                FieldPacket[] fields = new FieldPacket[fieldCount];
                fields[0] = PacketUtil.getField("SCHEMA_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[1] = PacketUtil.getField("DEFAULT_CHARACTER_SET_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[2] = PacketUtil.getField("DEFAULT_COLLATION_NAME", Fields.FIELD_TYPE_VAR_STRING);

                ServerConfig conf = DbleServer.getInstance().getConfig();
                Map<String, UserConfig> users = conf.getUsers();
                UserConfig user = users == null ? null : users.get(c.getUser());
                RowDataPacket[] rows = null;
                if (user != null) {
                    TreeSet<String> schemaSet = new TreeSet<>();
                    Set<String> schemaList = user.getSchemas();
                    if (schemaList == null || schemaList.size() == 0) {
                        schemaSet.addAll(conf.getSchemas().keySet());
                    } else {
                        for (String schema : schemaList) {
                            schemaSet.add(schema);
                        }
                    }

                    rows = new RowDataPacket[schemaSet.size()];
                    int index = 0;
                    for (String name : schemaSet) {
                        String charset = conf.getSystem().getCharset();
                        RowDataPacket row = new RowDataPacket(fieldCount);
                        row.add(StringUtil.encode(name, c.getCharset().getResults()));
                        row.add(StringUtil.encode(charset, c.getCharset().getResults()));
                        row.add(StringUtil.encode(CharsetUtil.getDefaultCollation(charset), c.getCharset().getResults()));
                        rows[index++] = row;
                    }
                }

                MysqlSystemSchemaHandler.doWrite(fieldCount, fields, rows, c);
            } else {
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            }
        }
    }
}
