/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * @author mycat
 */
public class MySQLDelayDetector extends MySQLDetector {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDelayDetector.class);
    private static final String[] MYSQL_DELAY_DETECTION_COLS = new String[]{
            "logic_timestamp",
    };

    public MySQLDelayDetector(MySQLHeartbeat heartbeat) {
        super(heartbeat);
        this.sqlJob = new HeartbeatSQLJob(heartbeat, new OneRawSQLQueryResultHandler(MYSQL_DELAY_DETECTION_COLS, this));
    }

    @Override
    protected void setStatus(PhysicalDbInstance source, Map<String, String> resultResult) {
        if (source.isReadInstance()) {
            String logicTimestamp = Optional.ofNullable(resultResult.get("logic_timestamp")).orElse("0");
            long logic = Long.parseLong(logicTimestamp);
            delayCal(logic, source.getDbGroupConfig().getDelayThreshold());
        } else {
            heartbeat.setSlaveBehindMaster(null);
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
        }
    }

    private void delayCal(long delay, long delayThreshold) {
        PhysicalDbGroup dbGroup = heartbeat.getSource().getDbGroup();
        long logic = dbGroup.getLogicTimestamp().get();
        long result = logic - delay;
        if (result >= 0) {
            long delayVal = result * (dbGroup.getDbGroupConfig().getDelayPeriodMillis() / 2);
            if (delayThreshold > 0 && delayVal > delayThreshold) {
                MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication delay !!! " + heartbeat.getSource().getConfig() + ", binlog sync time delay: " + delayVal + "ms");
            }
            heartbeat.setSlaveBehindMaster((int) delayVal);
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
        } else {
            // master and slave maybe switch
            heartbeat.setSlaveBehindMaster(null);
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_ERROR);
        }
    }
}
