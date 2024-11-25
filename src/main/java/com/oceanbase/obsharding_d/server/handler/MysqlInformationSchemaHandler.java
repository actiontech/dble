/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;

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
        ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
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
