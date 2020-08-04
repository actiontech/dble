/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.ShowServerLog;
import com.actiontech.dble.util.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Show @@SYSLOG LIMIT=50
 *
 * @author zhuam
 */
public final class ShowSysLog {
    private ShowSysLog() {
    }

    private static final int FIELD_COUNT = 2;

    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("DATE", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].setPacketId(++packetId);

        FIELDS[i] = PacketUtil.getField("LOG", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerService service, int numLines) {
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
        byte packetId = EOF.getPacketId();

        String filename = SystemConfig.getInstance().getHomePath() + File.separator + "logs" + File.separator + ShowServerLog.DEFAULT_LOGFILE;

        String[] lines = getLinesByLogFile(filename, numLines);

        boolean linesIsEmpty = true;
        for (String line : lines) {
            if (line != null) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(line.substring(0, 19), service.getCharset().getResults()));
                row.add(StringUtil.encode(line.substring(19, line.length()), service.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);

                linesIsEmpty = false;
            }
        }

        if (linesIsEmpty) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode("NULL", service.getCharset().getResults()));
            row.add(StringUtil.encode("NULL", service.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
        }

        // write last eof
        EOFRowPacket lastEof = new EOFRowPacket();
        lastEof.setPacketId(++packetId);


        lastEof.write(buffer, service);
    }

    private static String[] getLinesByLogFile(String filename, int numLines) {
        String[] lines = new String[numLines];
        BufferedReader in = null;
        try {
            int totalNumLines = 0;
            File logFile = new File(filename);
            in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                totalNumLines++;
            }
            in.close();


            in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8));

            // skip
            for (int i = 0; i < totalNumLines - numLines; i++) {
                in.readLine();
            }

            // DESC
            int i = 0;
            int end = lines.length - 1;

            while ((line = in.readLine()) != null && i < numLines) {
                lines[end - i] = line;
                i++;
            }

        } catch (FileNotFoundException ex) {
            //ignore error
        } catch (IOException e) {
            //ignore error
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
        return lines;
    }

}
