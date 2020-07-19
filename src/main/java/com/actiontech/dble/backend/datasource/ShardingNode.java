/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ShardingNode {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingNode.class);

    protected final String name;
    private final String dbGroupName;
    protected String database;
    protected volatile PhysicalDbGroup dbGroup;
    private volatile boolean isSchemaExists = true;

    public ShardingNode(String dbGroupName, String hostName, String database, PhysicalDbGroup dbGroup) {
        this.dbGroupName = dbGroupName;
        this.name = hostName;
        this.database = database;
        this.dbGroup = dbGroup;
    }

    public boolean isSchemaExists() {
        return isSchemaExists;
    }

    public void setSchemaExists(boolean schemaExists) {
        isSchemaExists = schemaExists;
    }

    public String getName() {
        return name;
    }

    public String getDbGroupName() {
        return dbGroupName;
    }

    public PhysicalDbGroup getDbGroup() {
        return dbGroup;
    }

    public void setDbGroup(PhysicalDbGroup dbGroup) {
        this.dbGroup = dbGroup;
    }

    public String getDatabase() {
        return database;
    }

    public void toLowerCase() {
        this.database = database.toLowerCase();
    }

    /**
     * get connection from the same dbInstance
     */
    public void getConnectionFromSameSource(String schema, BackendConnection exitsCon, ResponseHandler handler,
                                            Object attachment) throws Exception {

        PhysicalDbInstance ds = this.dbGroup.findDbInstance(exitsCon);
        if (ds == null) {
            throw new RuntimeException("can't find exits connection, maybe finished " + exitsCon);
        } else {
            ds.getConnection(schema, handler, attachment, false);
        }
    }

    private void checkRequest(String schema) {
        if (schema != null && !schema.equals(this.database)) {
            throw new RuntimeException("invalid param ,connection request db is :" + schema +
                    " and schema db is " + this.database);
        }
    }

    public void getConnection(String schema, boolean isMustWrite, boolean autoCommit, RouteResultsetNode rrs,
                              ResponseHandler handler, Object attachment) throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-sharding-node");
        try {
            if (isMustWrite) {
                getWriteNodeConnection(schema, handler, attachment);
                return;
            }
            if (rrs.getRunOnSlave() == null) {
                if (rrs.canRunINReadDB(autoCommit)) {
                    dbGroup.getRWSplitCon(schema, handler, attachment);
                } else {
                    getWriteNodeConnection(schema, handler, attachment);
                }
            } else {
                if (rrs.getRunOnSlave()) {
                    if (!dbGroup.getReadCon(schema, handler, attachment)) {
                        throw new IllegalArgumentException("no valid read dbInstance in dbGroup:" + dbGroup.getGroupName());
                    }
                } else {
                    rrs.setCanRunInReadDB(false);
                    getWriteNodeConnection(schema, handler, attachment);
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public BackendConnection getConnection(String schema, Boolean runOnSlave, Object attachment) throws IOException {
        if (runOnSlave == null) {
            PhysicalDbInstance readSource = dbGroup.getRWSplitNode();
            if (!readSource.isAlive()) {
                String heartbeatError = "the dbInstance[" + readSource.getConfig().getUrl() + "] can't reach. Please check the dbInstance status";
                if (dbGroup.getDbGroupConfig().isShowSlaveSql()) {
                    heartbeatError += ",Tip:heartbeat[show slave status] need the SUPER or REPLICATION CLIENT privilege(s)";
                }
                LOGGER.warn(heartbeatError);
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", readSource.getDbGroupConfig().getName() + "-" + readSource.getConfig().getInstanceName());
                AlertUtil.alert(AlarmCode.DB_INSTANCE_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", readSource.getConfig().getId(), labels);
                throw new IOException(heartbeatError);
            }
            return readSource.getConnection(schema, attachment);
        } else if (runOnSlave) {
            PhysicalDbInstance source = dbGroup.getRandomAliveReadNode();
            if (source == null) {
                throw new IllegalArgumentException("no valid dbInstance in dbGroup:" + dbGroup.getGroupName());
            }
            return source.getConnection(schema, attachment);
        } else {
            checkRequest(schema);
            PhysicalDbInstance writeSource = dbGroup.getWriteDbInstance();
            if (writeSource.isReadOnly()) {
                throw new IllegalArgumentException("The dbInstance[" + writeSource.getConfig().getUrl() + "] is running with the --read-only option so it cannot execute this statement");
            }
            writeSource.incrementWriteCount();
            return writeSource.getConnection(schema, attachment);
        }
    }

    private void getWriteNodeConnection(String schema, ResponseHandler handler, Object attachment) throws IOException {
        checkRequest(schema);
        PhysicalDbInstance writeSource = dbGroup.getWriteDbInstance();
        if (writeSource.isDisabled()) {
            throw new IllegalArgumentException("[" + writeSource.getDbGroupConfig().getName() + "." + writeSource.getConfig().getInstanceName() + "] is disabled");
        } else if (writeSource.isFakeNode()) {
            throw new IllegalArgumentException("[" + writeSource.getDbGroupConfig().getName() + "." + writeSource.getConfig().getInstanceName() + "] is fake node");
        }
        if (writeSource.isReadOnly()) {
            throw new IllegalArgumentException("The dbInstance[" + writeSource.getConfig().getUrl() + "] is running with the --read-only option so it cannot execute this statement");
        }
        writeSource.incrementWriteCount();
        writeSource.getConnection(schema, handler, attachment, true);
    }
}
