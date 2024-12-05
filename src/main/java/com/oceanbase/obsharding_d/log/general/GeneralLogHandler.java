/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;

import com.oceanbase.obsharding_d.backend.mysql.MySQLMessage;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;

import java.io.UnsupportedEncodingException;

public final class GeneralLogHandler {
    private GeneralLogHandler() {
    }


    public static String[] packageLog(byte[] data, String charset) {
        String type, content = null;
        String[] arr = new String[2];
        arr[0] = (type = MySQLPacket.TO_STRING.get(data[4])) == null ? "UNKNOWN" : type;
        arr[1] = null;
        try {
            switch (data[4]) {
                case MySQLPacket.COM_STMT_RESET:
                case MySQLPacket.COM_STMT_CLOSE:
                case MySQLPacket.COM_RESET_CONNECTION:
                case MySQLPacket.COM_CHANGE_USER:
                case MySQLPacket.COM_PING:
                case MySQLPacket.COM_HEARTBEAT:
                case MySQLPacket.COM_QUIT:
                case MySQLPacket.COM_STMT_SEND_LONG_DATA:
                case MySQLPacket.COM_PROCESS_KILL:
                case MySQLPacket.COM_STMT_EXECUTE:
                    //case MySQLPacket.COM_SET_OPTION:
                    break;
                case MySQLPacket.COM_QUERY:
                case MySQLPacket.COM_INIT_DB:
                case MySQLPacket.COM_STMT_PREPARE:
                    MySQLMessage mm = new MySQLMessage(data);
                    mm.position(5);
                    content = mm.readString(charset);
                    break;
                case MySQLPacket.COM_FIELD_LIST:
                    MySQLMessage mm2 = new MySQLMessage(data);
                    mm2.position(5);
                    content = mm2.readStringWithNull();
                    break;
                default:
                    break;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        arr[1] = content;
        return arr;
    }
}
