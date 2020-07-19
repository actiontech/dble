package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.mysql.EOFRowPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;

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

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            service.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(service.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            service.writeErrMessage("42000", "Access denied for user '" + service.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
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
