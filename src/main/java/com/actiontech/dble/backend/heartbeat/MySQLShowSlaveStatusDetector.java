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
public class MySQLShowSlaveStatusDetector extends MySQLDetector {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLShowSlaveStatusDetector.class);
    private static final String[] MYSQL_SLAVE_STATUS_COLS = new String[]{
            "Seconds_Behind_Master",
            "Slave_IO_Running",
            "Slave_SQL_Running",
            "Slave_IO_State",
            "Master_Host",
            "Master_User",
            "Master_Port",
            "Connect_Retry",
            "Last_IO_Error"};

    public MySQLShowSlaveStatusDetector(MySQLHeartbeat heartbeat) {
        super(heartbeat);
        this.sqlJob = new HeartbeatSQLJob(heartbeat, new OneRawSQLQueryResultHandler(MYSQL_SLAVE_STATUS_COLS, this));
    }

    @Override
    protected void setStatus(PhysicalDbInstance source, Map<String, String> resultResult) {
        String slaveIoRunning = resultResult != null ? resultResult.get("Slave_IO_Running") : null;
        String slaveSqlRunning = resultResult != null ? resultResult.get("Slave_SQL_Running") : null;
        if (slaveIoRunning != null && slaveIoRunning.equals(slaveSqlRunning) && slaveSqlRunning.equals("Yes")) {
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
            String secondsBehindMaster = resultResult.get("Seconds_Behind_Master");
            if (null != secondsBehindMaster && !"".equals(secondsBehindMaster) && !"NULL".equalsIgnoreCase(secondsBehindMaster)) {
                int behindMaster = Integer.parseInt(secondsBehindMaster) * 1000;
                int delayThreshold = source.getDbGroupConfig().getDelayThreshold();
                if (delayThreshold > 0 && behindMaster > delayThreshold) {
                    MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication delay !!! " + heartbeat.getSource().getConfig() + ", binlog sync time delay: " + behindMaster + "ms");
                }
                heartbeat.setSlaveBehindMaster(behindMaster);
            } else {
                heartbeat.setSlaveBehindMaster(null);
            }
        } else if (source.isSalveOrRead()) {
            //String Last_IO_Error = resultResult != null ? resultResult.get("Last_IO_Error") : null;
            MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication err !!! " +
                    heartbeat.getSource().getConfig() + ", " + resultResult);
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_ERROR);
            heartbeat.setSlaveBehindMaster(null);
        }
        heartbeat.getAsyncRecorder().setBySlaveStatus(resultResult);
    }
}
