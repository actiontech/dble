/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.handler.ShowServerLog;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;

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
        FIELDS[i++].setPacketId(++packetId);

        EOF.setPacketId(++packetId);
    }

    public static void execute(ManagerConnection c, int numLines) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.getPacketId();

        String filename = SystemConfig.getHomePath() + File.separator + "logs" + File.separator + ShowServerLog.DEFAULT_LOGFILE;

        String[] lines = getLinesByLogFile(filename, numLines);

        boolean linesIsEmpty = true;
        for (String line : lines) {
            if (line != null) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(line.substring(0, 19), c.getCharset().getResults()));
                row.add(StringUtil.encode(line.substring(19, line.length()), c.getCharset().getResults()));
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);

                linesIsEmpty = false;
            }
        }

        if (linesIsEmpty) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode("NULL", c.getCharset().getResults()));
            row.add(StringUtil.encode("NULL", c.getCharset().getResults()));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, c, true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static String[] getLinesByLogFile(String filename, int numLines) {
        String[] lines = new String[numLines];
        BufferedReader in = null;
        try {
            int totalNumLines = 0;
            File logFile = new File(filename);
            in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                totalNumLines++;
            }
            in.close();


            in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));

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
