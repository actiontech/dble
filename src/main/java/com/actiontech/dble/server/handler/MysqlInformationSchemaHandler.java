/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
     * @param service
     * @param fields
     */
    public static void handle(ShardingService service, FieldPacket[] fields) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        Map<UserName, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(service.getUser());
        RowDataPacket[] rows = null;
        if (user != null) {
            ShardingUserConfig shardingUser = (ShardingUserConfig) user;
            TreeSet<String> schemaSet = new TreeSet<>();
            Set<String> schemaList = shardingUser.getSchemas();
            if (schemaList == null || schemaList.size() == 0) {
                schemaSet.addAll(conf.getSchemas().keySet());
            } else {
                schemaSet.addAll(schemaList);
            }

            rows = new RowDataPacket[schemaSet.size()];
            int index = 0;
            for (String name : schemaSet) {
                String charset = SystemConfig.getInstance().getCharset();
                RowDataPacket row = new RowDataPacket(fields.length);
                for (int j = 0; j < fields.length; j++) {
                    String columnName = StringUtil.decode(fields[j].getName(), service.getCharset().getResults());
                    switch (columnName.toUpperCase()) {
                        case "SCHEMA_NAME":
                            row.add(StringUtil.encode(name, service.getCharset().getResults()));
                            break;
                        case "DEFAULT_CHARACTER_SET_NAME":
                            row.add(StringUtil.encode(charset, service.getCharset().getResults()));
                            break;
                        case "DEFAULT_COLLATION_NAME":
                            row.add(StringUtil.encode(CharsetUtil.getDefaultCollation(charset), service.getCharset().getResults()));
                            break;
                        default:
                            break;
                    }
                }
                rows[index++] = row;
            }
        }

        MysqlSystemSchemaHandler.doWrite(fields.length, fields, rows, service);
    }
}
