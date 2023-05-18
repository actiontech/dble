/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.rwsplit.Callback;
import com.actiontech.dble.services.rwsplit.RWSplitHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.StringUtil;

import java.util.*;

public class RwSplitSelectVariablesHandler extends RWSplitHandler {
    private CharsetNames charsetName;
    private List<String> columnNames = new ArrayList<>();

    public RwSplitSelectVariablesHandler(RWSplitService service, boolean isUseOriginPacket, byte[] originPacket, Callback callback) {
        super(service, isUseOriginPacket, originPacket, callback);
        charsetName = service.getCharset();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        for (byte[] field : fields) {
            FieldPacket fieldPacket = new FieldPacket();
            fieldPacket.read(field);
            String columnName = StringUtil.decode(fieldPacket.getName(), charsetName.getResults());
            columnNames.add(columnName);
        }
        super.fieldEofResponse(header, fields, fieldPacketsNull, eof, isLeft, service);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        int index = columnNames.size();
        RowDataPacket rowDataPacket = new RowDataPacket(index);
        String charset = charsetName.getResults();
        rowDataPacket.read(row);
        Map<String, String> variables = getVariables();
        for (int i = 0; i < index; i++) {
            String columnName = columnNames.get(i);
            if (variables.containsKey(columnName)) {
                rowDataPacket.setValue(i, StringUtil.encode(variables.get(columnName), charset));
            }
        }
        return super.rowResponse(rowDataPacket.toBytes(), rowPacket, isLeft, service);
    }

    private Map<String, String> getVariables() {
        Map<String, String> variables = new HashMap<>();
        variables.put("character_set_client", charsetName.getClient());
        variables.put("collation_connection", charsetName.getCollation());
        variables.put("character_set_results", charsetName.getResults());
        variables.put("character_set_connection", charsetName.getCollation());
        return variables;
    }
}
