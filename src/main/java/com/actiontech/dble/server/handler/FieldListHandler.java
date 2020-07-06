package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.ProxyMeta;

import java.nio.ByteBuffer;
import java.util.List;

public final class FieldListHandler {
    private FieldListHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        String cSchema = c.getSchema();
        if (cSchema == null) {
            c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
            return;
        }

        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(cSchema);
        if (schema == null) {
            c.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
            return;
        }

        ShardingUserConfig user = (ShardingUserConfig) (DbleServer.getInstance().getConfig().getUsers().get(c.getUser()));
        if (user == null || !user.getSchemas().contains(cSchema)) {
            c.writeErrMessage("42000", "Access denied for user '" + c.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
            return;
        }

        String table = stmt.trim();
        SchemaMeta schemata = null;
        TableMeta tableMeta = null;

        if (!schema.getTables().containsKey(table) || (schemata = ProxyMeta.getInstance().getTmManager().getCatalogs().get(cSchema)) == null || (tableMeta = schemata.getTableMeta(table)) == null) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The table [" + cSchema + "." + table + "] doesn't exist");
            return;
        }

        List<TableMeta.ColumnMeta> columns = tableMeta.getColumns();
        FieldPacket[] fields = new FieldPacket[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            fields[i] = PacketUtil.getField(columns.get(i).getName(), columns.get(i).getDataType().contains("char") ? Fields.FIELD_TYPE_VAR_STRING : Fields.FIELD_TYPE_LONG);
        }
        doWrite(c, fields);
    }

    private static void doWrite(ServerConnection c, FieldPacket[] fields) {
        byte packetId = 0;
        ByteBuffer buffer = c.allocate();

        // write fields
        for (FieldPacket field : fields) {
            field.setPacketId(++packetId);
            buffer = field.write(buffer, c, true);
        }

        // EOF
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, c, true);
        c.write(buffer);
    }
}
