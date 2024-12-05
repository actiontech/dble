/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GeneralLogEntry extends LogEntry {
    // 1: connID、data、charset
    // 2: connID、command、content
    // 3: content
    private int entryType = 1;
    private long connID;
    byte[] data;
    private String charset;
    private String command;
    private String content = null;

    GeneralLogEntry(long connID, byte[] data, String charset) {
        super();
        this.entryType = 1;
        this.connID = connID;
        this.charset = charset;
        this.data = data;
    }

    GeneralLogEntry(long connID, String command, String content) {
        super();
        this.entryType = 2;
        this.connID = connID;
        this.command = command;
        this.content = content;
    }

    GeneralLogEntry(String content) {
        super();
        this.entryType = 3;
        this.content = content;
    }

    public String toLog() {
        return toPackag();
    }

    private String toPackag() {
        switch (entryType) {
            case 1:
                String[] arr = GeneralLogHandler.packageLog(data, charset);
                command = arr[0];
                content = arr[1];
                return toLogString();
            case 2:
                return toLogString();
            case 3:
                return content;
            default:
                break;
        }
        return "";
    }

    private String toLogString() {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        StringBuilder sb = new StringBuilder();
        sb.append(dataFormat.format(new Date(super.time)));
        sb.append("\t ");
        sb.append(connID);
        sb.append(" ");
        sb.append(command);
        if (content != null) {
            sb.append("\t");
            sb.append(content);
        }
        sb.append("\n");
        return sb.toString();
    }
}

