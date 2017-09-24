/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

/**
 * Capability flag
 *
 * @author mycat
 */
public final class Capabilities {
    private Capabilities() {
    }

    /**
     * server capabilities
     * <p>
     * <pre>
     * server:        11110111 11111111
     * client_cmd: 11 10100110 10000101
     * client_jdbc:10 10100010 10001111
     *
     * @see http://dev.mysql.com/doc/refman/5.1/en/mysql-real-connect.html
     * </pre>
     */
    // new more secure passwords
    public static final int CLIENT_LONG_PASSWORD = 1;

    // return the number of found (matched) rows, not the number of changed rows.
    public static final int CLIENT_FOUND_ROWS = 2;

    // Get all column flags
    public static final int CLIENT_LONG_FLAG = 4;

    // One can specify db on connect
    public static final int CLIENT_CONNECT_WITH_DB = 8;

    // Don't allow database.table.column
    public static final int CLIENT_NO_SCHEMA = 16;

    // Can use compression protocol
    public static final int CLIENT_COMPRESS = 32;

    // Odbc client
    public static final int CLIENT_ODBC = 64;

    // Can use LOAD DATA LOCAL
    public static final int CLIENT_LOCAL_FILES = 128;

    // Permit spaces after function names. Makes all functions names reserved words.
    public static final int CLIENT_IGNORE_SPACE = 256;

    // New 4.1 protocol This is an interactive client
    public static final int CLIENT_PROTOCOL_41 = 512;

    // Permit interactive_timeout seconds of inactivity (rather than wait_timeout seconds) before closing the connection
    public static final int CLIENT_INTERACTIVE = 1024;

    // Use SSL (encrypted protocol).
    // Do not set this option within an application program;
    // it is set internally in the client library.
    // Instead, use mysql_options() or mysql_ssl_set() before calling mysql_real_connect().
    public static final int CLIENT_SSL = 2048;

    // Prevents the client library from installing a SIGPIPE signal handler.
    // This can be used to avoid conflicts with a handler that the application has already installed.
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;

    // Client knows about transactions
    public static final int CLIENT_TRANSACTIONS = 8192;

    // Old flag for 4.1 protocol
    public static final int CLIENT_RESERVED = 16384;

    // New 4.1 authentication
    public static final int CLIENT_SECURE_CONNECTION = 32768;

    // Enable/disable multi-stmt support
    public static final int CLIENT_MULTI_STATEMENTS = 65536;

    // Enable/disable multi-results
    public static final int CLIENT_MULTI_RESULTS = 131072;

    public static final int CLIENT_PLUGIN_AUTH = 0x00080000; // 524288

}
