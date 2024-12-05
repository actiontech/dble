/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.mysql;

import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;

/**
 * @author mycat
 */
public abstract class MySQLPacket {
    /**
     * none, this is an internal thread state
     */
    public static final byte COM_SLEEP = 0;

    /**
     * mysql_close
     */
    public static final byte COM_QUIT = 1;

    /**
     * mysql_select_db
     */
    public static final byte COM_INIT_DB = 2;

    /**
     * mysql_real_query
     */
    public static final byte COM_QUERY = 3;

    /**
     * mysql_list_fields
     */
    public static final byte COM_FIELD_LIST = 4;

    /**
     * mysql_create_db (deprecated)
     */
    public static final byte COM_CREATE_DB = 5;

    /**
     * mysql_drop_db (deprecated)
     */
    public static final byte COM_DROP_DB = 6;

    /**
     * mysql_refresh
     */
    public static final byte COM_REFRESH = 7;

    /**
     * mysql_shutdown
     */
    public static final byte COM_SHUTDOWN = 8;

    /**
     * mysql_stat
     */
    public static final byte COM_STATISTICS = 9;

    /**
     * mysql_list_processes
     */
    public static final byte COM_PROCESS_INFO = 10;

    /**
     * none, this is an internal thread state
     */
    public static final byte COM_CONNECT = 11;

    /**
     * mysql_kill
     */
    public static final byte COM_PROCESS_KILL = 12;

    /**
     * mysql_dump_debug_info
     */
    public static final byte COM_DEBUG = 13;

    /**
     * mysql_ping
     */
    public static final byte COM_PING = 14;

    /**
     * none, this is an internal thread state
     */
    public static final byte COM_TIME = 15;

    /**
     * none, this is an internal thread state
     */
    public static final byte COM_DELAYED_INSERT = 16;

    /**
     * mysql_change_user
     */
    public static final byte COM_CHANGE_USER = 17;

    /**
     * used by slave server mysqlbinlog
     */
    public static final byte COM_BINLOG_DUMP = 18;

    /**
     * used by slave server to get master table
     */
    public static final byte COM_TABLE_DUMP = 19;

    /**
     * used by slave to log connection to master
     */
    public static final byte COM_CONNECT_OUT = 20;

    /**
     * used by slave to register to master
     */
    public static final byte COM_REGISTER_SLAVE = 21;

    /**
     * mysql_stmt_prepare
     */
    public static final byte COM_STMT_PREPARE = 22;

    /**
     * mysql_stmt_execute
     */
    public static final byte COM_STMT_EXECUTE = 23;

    /**
     * mysql_stmt_send_long_data
     */
    public static final byte COM_STMT_SEND_LONG_DATA = 24;

    /**
     * mysql_stmt_close
     */
    public static final byte COM_STMT_CLOSE = 25;

    /**
     * mysql_stmt_reset
     */
    public static final byte COM_STMT_RESET = 26;

    /**
     * mysql_set_server_option
     */
    public static final byte COM_SET_OPTION = 27;

    /**
     * mysql_stmt_fetch
     */
    public static final byte COM_STMT_FETCH = 28;
    /**
     * mysql-reset-connection
     */
    public static final byte COM_RESET_CONNECTION = 31;

    /**
     * heartbeat
     */
    public static final byte COM_HEARTBEAT = 64;

    //HEADER_SIZE
    public static final int PACKET_HEADER_SIZE = 4;
    //2^24-1
    public static final int MAX_PACKET_SIZE = 16777215;

    public static final int MAX_EOF_SIZE = 9;


    protected int packetLength;
    protected byte packetId;

    /**
     * writeDirectly to buffer ,if writeSocketIfFull writeDirectly the buffer data to FrontendConnection
     */
    public ByteBuffer write(ByteBuffer buffer, AbstractService service, boolean writeSocketIfFull) {
        throw new UnsupportedOperationException();
    }

    /**
     * writeDirectly to backend connection
     */
    public void write(MySQLResponseService service) {
        this.write(service.getConnection());
    }

    /**
     * writeDirectly to a net connection
     */
    public final void write(AbstractConnection connection) {
        connection.getService().write(this);
    }


    public void bufferWrite(AbstractConnection connection) {
        ByteBuffer buffer = connection.allocate();
        buffer = this.write(buffer, connection.getService(), true);
        connection.getService().writeDirectly(buffer, getLastWriteFlag(), getResultFlag());
    }


    public void bufferWrite(AbstractService service) {
        ByteBuffer buffer = service.allocate();
        buffer = this.write(buffer, service, true);
        service.writeDirectly(buffer, getLastWriteFlag());
    }


    public final EnumSet<WriteFlag> getLastWriteFlag() {
        if (isEndOfSession()) {
            return WriteFlags.SESSION_END;
        } else if (isEndOfQuery()) {
            return WriteFlags.QUERY_END;
        } else {
            return WriteFlags.PART;
        }
    }

    public ResultFlag getResultFlag() {
        return ResultFlag.OTHER;
    }

    /**
     * calcPacketSize,not contains header size
     */
    public abstract int calcPacketSize();

    /**
     * getPacketInfo
     */
    protected abstract String getPacketInfo();


    @Override
    public String toString() {
        return getPacketInfo() + "{length=" + packetLength + ",id=" +
                packetId + '}';
    }

    public void setPacketLength(int packetLength) {
        this.packetLength = packetLength;
    }

    public byte getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetID) {
        this.packetId = (byte) packetID;
    }

    public abstract boolean isEndOfQuery();

    public boolean isEndOfSession() {
        return false;
    }

    public void markMoreResultsExists() {
    }

    public static final HashMap<Byte, String> TO_STRING = new HashMap(18);

    static {
        // see: mysql-5.7.20/sql/sql_parse.cc
        TO_STRING.put(COM_SLEEP, "Sleep");
        TO_STRING.put(COM_QUIT, "Quit");
        TO_STRING.put(COM_INIT_DB, "Init DB");
        TO_STRING.put(COM_QUERY, "Query");
        TO_STRING.put(COM_FIELD_LIST, "Field List");
        TO_STRING.put(COM_CONNECT, "Connect");
        TO_STRING.put(COM_PROCESS_KILL, "Kill");
        TO_STRING.put(COM_PING, "Ping");
        TO_STRING.put(COM_DELAYED_INSERT, "Delayed insert");
        TO_STRING.put(COM_CHANGE_USER, "Change user");
        TO_STRING.put(COM_REGISTER_SLAVE, "Register Slave");
        TO_STRING.put(COM_STMT_PREPARE, "Prepare");
        TO_STRING.put(COM_STMT_EXECUTE, "Execute");
        TO_STRING.put(COM_STMT_SEND_LONG_DATA, "Long Data");
        TO_STRING.put(COM_STMT_CLOSE, "Close stmt");
        TO_STRING.put(COM_STMT_RESET, "Reset stmt");
        TO_STRING.put(COM_SET_OPTION, "Set option");
        TO_STRING.put(COM_RESET_CONNECTION, "Reset Connection");
        TO_STRING.put(COM_HEARTBEAT, "Heartbeat");
    }
}
