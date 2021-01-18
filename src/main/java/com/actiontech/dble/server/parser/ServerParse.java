/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.parser;

/**
 * @author dcy
 * Create Date: 2021-01-12
 */
public interface ServerParse {
    /**
     * split sharding and rwSplit  if  possible
     */
    int OTHER = -1;
    int BEGIN = 1;
    int COMMIT = 2;
    int DELETE = 3;
    int INSERT = 4;
    int REPLACE = 5;
    int ROLLBACK = 6;
    int SELECT = 7;
    int SET = 8;
    int SHOW = 9;
    int START = 10;
    int UPDATE = 11;
    int KILL = 12;
    int SAVEPOINT = 13;
    int USE = 14;
    int EXPLAIN = 15;
    int KILL_QUERY = 16;
    int HELP = 17;
    int MYSQL_CMD_COMMENT = 18;
    int MYSQL_COMMENT = 19;
    int CALL = 20;
    int DESCRIBE = 21;
    int LOCK = 22;
    int UNLOCK = 23;
    int CREATE_VIEW = 24;
    int REPLACE_VIEW = 25;
    int ALTER_VIEW = 27;
    int DROP_VIEW = 26;
    int LOAD_DATA_INFILE_SQL = 99;
    int DDL = 100;
    int SCRIPT_PREPARE = 101;
    int EXPLAIN2 = 151;
    int CREATE_DATABASE = 152;
    int FLUSH = 153;
    int ROLLBACK_SAVEPOINT = 154;
    int RELEASE_SAVEPOINT = 155;
    int SELECT_FOR_UPDATE = 156;
    int LOCK_IN_SHARE_MODE = 157;
    int CREATE_TEMPORARY_TABLE = 158;
    int DROP_TABLE = 159;

    int MIGRATE = 203;
    /* don't set the constant to 255 */
    int UNSUPPORT = 254;


    int parse(String stmt);

    int parseSpecial(int sqlType, String stmt);

    boolean startWithHint(String stmt);

    boolean isMultiStatement(String sql);
}
