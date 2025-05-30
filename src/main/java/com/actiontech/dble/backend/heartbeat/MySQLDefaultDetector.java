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
public class MySQLDefaultDetector extends MySQLDetector {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDefaultDetector.class);

    public MySQLDefaultDetector(MySQLHeartbeat heartbeat) {
        super(heartbeat);
        String[] fetchCols = {};
        this.sqlJob = new HeartbeatSQLJob(heartbeat, new OneRawSQLQueryResultHandler(fetchCols, this));
    }

    @Override
    protected void setStatus(PhysicalDbInstance source, Map<String, String> resultResult) {
        // heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
    }
}
