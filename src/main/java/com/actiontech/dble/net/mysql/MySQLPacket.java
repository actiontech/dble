/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.mysql;

import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.nio.ByteBuffer;

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

    public abstract void bufferWrite(AbstractConnection connection);

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

    public boolean isEndOfQuery() {
        return false;
    }

    public boolean isEndOfSession() {
        return false;
    }

    public void markMoreResultsExists() {
    }
}
