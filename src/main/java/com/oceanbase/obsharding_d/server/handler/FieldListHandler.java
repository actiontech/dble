/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.net.mysql.EOFRowPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;

import java.nio.ByteBuffer;
import java.util.List;

public final class FieldListHandler {
    private FieldListHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        String cSchema = service.getSchema();
        if (cSchema == null) {
            service.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }

        SchemaConfig schema = OBsharding_DServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            service.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (OBsharding_DServer.getInstance().getConfig().getUsers().get(service.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            service.writeErrMessage("42000", "Access denied for user '" + service.getUser().getFullName() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String table = stmt.trim();
        SchemaMeta schemata = null;
        TableMeta tableMeta = null;

        if ((schemata = ProxyMeta.getInstance().getTmManager().getCatalogs().get(cSchema)) == null || (tableMeta = schemata.getTableMeta(table)) == null) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
            return;
        }

        List<ColumnMeta> columns = tableMeta.getColumns();
        FieldPacket[] fields = new FieldPacket[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fields[i] = PacketUtil.getField(columns.get(i).getName(), getFieldType(columns.get(i).getDataType()));
        }
        doWrite(service, fields);
    }

    private static int getFieldType(String dataType) {
        switch (dataType) {
            case "tinyint":
                return Fields.FIELD_TYPE_TINY;
            case "smallint":
                return Fields.FIELD_TYPE_SHORT;
            case "mediumint":
                return Fields.FIELD_TYPE_INT24;
            case "int":
            case "integer":
                return Fields.FIELD_TYPE_LONG;
            case "bigint":
                return Fields.FIELD_TYPE_LONGLONG;
            case "float":
                return Fields.FIELD_TYPE_FLOAT;
            case "double":
                return Fields.FIELD_TYPE_DOUBLE;
            case "decimal":
                return Fields.FIELD_TYPE_NEW_DECIMAL;
            case "date":
                return Fields.FIELD_TYPE_DATE;
            case "time":
                return Fields.FIELD_TYPE_TIME;
            case "year":
                return Fields.FIELD_TYPE_YEAR;
            case "datetime":
                return Fields.FIELD_TYPE_DATETIME;
            case "timestamp":
                return Fields.FIELD_TYPE_TIMESTAMP;
            case "char":
                return Fields.FIELD_TYPE_STRING;
            case "varchar":
                return Fields.FIELD_TYPE_VAR_STRING;
            case "tinyblob":
            case "tinytext":
            case "blob":
            case "text":
            case "mediumblob":
            case "mediumtext":
            case "longblob":
            case "longtext":
                return Fields.FIELD_TYPE_BLOB;
            default:
                return Fields.FIELD_TYPE_DECIMAL;
        }
    }

    private static void doWrite(ShardingService service, FieldPacket[] fields) {
        byte packetId = 0;
        ByteBuffer buffer = service.allocate();

        // write fields
        for (FieldPacket field : fields) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, service, true);
        }

        // EOF
        EOFRowPacket eof = new EOFRowPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, service, true);
        eof.write(buffer, service);
    }
}
