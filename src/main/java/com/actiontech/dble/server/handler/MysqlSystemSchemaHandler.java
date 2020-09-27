/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.nio.ByteBuffer;
import java.util.List;

public final class MysqlSystemSchemaHandler {
    private MysqlSystemSchemaHandler() {
    }

    public static final String SCHEMATA_TABLE = "SCHEMATA";
    public static final String COLUMNS_TABLE = "COLUMNS";
    public static final String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";

    public static void handle(ShardingService service, SchemaUtil.SchemaInfo schemaInfo, SQLSelectQuery sqlSelectQuery) {
        MySqlSelectQueryBlock mySqlSelectQueryBlock = null;
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            mySqlSelectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery mySqlSelectUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            mySqlSelectQueryBlock = (MySqlSelectQueryBlock) mySqlSelectUnionQuery.getFirstQueryBlock();
        }

        if (mySqlSelectQueryBlock == null) {
            service.writeDirectly(service.writeToBuffer(OkPacket.OK, service.allocate()));
            return;
        }

        FieldPacket[] fields = generateFieldPacket(mySqlSelectQueryBlock.getSelectList());
        if (schemaInfo != null && INFORMATION_SCHEMA.equals(schemaInfo.getSchema().toUpperCase())) {
            switch (schemaInfo.getTable().toUpperCase()) {
                case SCHEMATA_TABLE:
                    MysqlInformationSchemaHandler.handle(service, fields);
                    return;
                case COLUMNS_TABLE:
                    new SelectInformationSchemaColumnsHandler().handle(service, fields, mySqlSelectQueryBlock, schemaInfo.getTableAlias());
                    return;
                default:
                    break;
            }
        }

        doWrite(fields.length, fields, null, service);
    }

    private static FieldPacket[] generateFieldPacket(List<SQLSelectItem> selectList) {
        FieldPacket[] fields = new FieldPacket[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            String columnName;
            SQLSelectItem selectItem = selectList.get(i);
            if (selectItem.getAlias() != null) {
                columnName = StringUtil.removeBackQuote(selectList.get(i).getAlias());
            } else {
                columnName = StringUtil.removeBackQuote(selectItem.toString());
            }
            fields[i] = PacketUtil.getField(columnName, Fields.FIELD_TYPE_VAR_STRING);
        }
        return fields;
    }

    /**
     * @param fieldCount
     * @param fields
     * @param service
     */
    public static void doWrite(int fieldCount, FieldPacket[] fields, RowDataPacket[] rows, ShardingService service) {

        ByteBuffer buffer = service.allocate();

        // writeDirectly header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        header.setPacketId(service.nextPacketId());
        buffer = header.write(buffer, service, true);

        // writeDirectly fields
        for (FieldPacket field : fields) {
            field.setPacketId(service.nextPacketId());
            buffer = field.write(buffer, service, true);
        }

        // writeDirectly eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(service.nextPacketId());
        buffer = eof.write(buffer, service, true);

        // writeDirectly rows
        if (rows != null) {
            for (RowDataPacket row : rows) {
                row.setPacketId(service.nextPacketId());
                buffer = row.write(buffer, service, true);
            }
        }

        // writeDirectly last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(service.nextPacketId());

        lastEof.write(buffer, service);
    }
}

