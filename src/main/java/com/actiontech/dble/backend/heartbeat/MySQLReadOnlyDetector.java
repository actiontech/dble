/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author mycat
 */
public class MySQLReadOnlyDetector extends MySQLDetector {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLReadOnlyDetector.class);
    private static final String[] MYSQL_READ_ONLY_COLS = new String[]{"@@read_only"};

    public MySQLReadOnlyDetector(MySQLHeartbeat heartbeat) {
        super(heartbeat);
        this.sqlJob = new HeartbeatSQLJob(heartbeat, new OneRawSQLQueryResultHandler(MYSQL_READ_ONLY_COLS, this));
    }

    @Override
    protected void setStatus(PhysicalDbInstance source, Map<String, String> resultResult) {
        String readonly = resultResult != null ? resultResult.get("@@read_only") : null;
        if (readonly == null) {
            heartbeat.setErrorResult("result of select @@read_only is null");
        } else if (readonly.equals("0")) {
            source.setReadOnly(false);
        } else {
            source.setReadOnly(true);
        }
    }
}
