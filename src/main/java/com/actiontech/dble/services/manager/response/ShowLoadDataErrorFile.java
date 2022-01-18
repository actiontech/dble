/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.actiontech.dble.backend.mysql.PacketUtil.getField;
import static com.actiontech.dble.backend.mysql.PacketUtil.getHeader;

public final class ShowLoadDataErrorFile {
    private ShowLoadDataErrorFile() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void execute(ManagerService service) {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = getField("error_load_data_file", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows
        String filePath = SystemConfig.getInstance().getHomePath() + File.separator + "temp" + File.separator + "error";
        List<String> errorFileList = new ArrayList<>();
        getErrorFile(filePath, errorFileList);
        for (String errorFilePath : errorFileList) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(errorFilePath.getBytes());
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }
        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);
        lastEof.write(buffer, service);
    }

    private static void getErrorFile(String filePath, List<String> errorFileList) {
        File dirFile = new File(filePath);
        if (!dirFile.exists()) {
            return;
        }
        File[] fileList = dirFile.listFiles();
        if (fileList != null) {
            for (File file : fileList) {
                if (file.isFile() && file.exists()) {
                    errorFileList.add(file.getPath());
                } else if (file.isDirectory()) {
                    getErrorFile(file.getPath(), errorFileList);
                }
            }
        }
    }
}
