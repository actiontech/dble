/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class PhysicalDBNode {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBNode.class);

    protected final String name;
    protected String database;
    protected volatile AbstractPhysicalDBPool dbPool;
    private volatile boolean isSchemaExists = true;

    public PhysicalDBNode(String hostName, String database, AbstractPhysicalDBPool dbPool) {
        this.name = hostName;
        this.database = database;
        this.dbPool = dbPool;
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

    public AbstractPhysicalDBPool getDbPool() {
        return dbPool;
    }

    public void setDbPool(AbstractPhysicalDBPool dbPool) {
        this.dbPool = dbPool;
    }

    public String getDatabase() {
        return database;
    }

    public void toLowerCase() {
        this.database = database.toLowerCase();
    }

    /**
     * get connection from the same datasource
     *
     * @param exitsCon
     * @throws Exception
     */
    public void getConnectionFromSameSource(String schema, boolean autocommit,
                                            BackendConnection exitsCon, ResponseHandler handler,
                                            Object attachment) throws Exception {

        PhysicalDatasource ds = this.dbPool.findDatasource(exitsCon);
        if (ds == null) {
            throw new RuntimeException("can't find exits connection, maybe finished " + exitsCon);
        } else {
            ds.getConnection(schema, autocommit, handler, attachment, false);
        }
    }

    private void checkRequest(String schema) {
        if (schema != null && !schema.equals(this.database)) {
            throw new RuntimeException("invalid param ,connection request db is :" + schema +
                    " and datanode db is " + this.database);
        }
        if (!dbPool.isInitSuccess() && !dbPool.init()) {
            throw new RuntimeException("DataNode[" + dbPool.getHostName() + "]'s init error, please check it can be connected. " +
                    "The current Node is {DataHost[" + dbPool.getSource().getConfig().getUrl() + ",Schema[" + schema + "]}");
        }
    }

    public void getConnection(String schema, boolean isMustWrite, boolean autoCommit, RouteResultsetNode rrs,
                              ResponseHandler handler, Object attachment) throws Exception {
        if (isMustWrite) {
            getWriteNodeConnection(schema, autoCommit, handler, attachment, false);
            return;
        }
        if (rrs.getRunOnSlave() == null) {
            if (rrs.canRunINReadDB(autoCommit)) {
                dbPool.getRWBalanceCon(schema, autoCommit, handler, attachment);
            } else {
                getWriteNodeConnection(schema, autoCommit, handler, attachment, false);
            }
        } else {
            if (rrs.getRunOnSlave()) {
                if (!dbPool.getReadCon(schema, autoCommit, handler, attachment)) {
                    LOGGER.info("Do not have slave connection to use, use master connection instead.");
                    rrs.setRunOnSlave(false);
                    rrs.setCanRunInReadDB(false);
                    getWriteNodeConnection(schema, autoCommit, handler, attachment, true);
                }
            } else {
                rrs.setCanRunInReadDB(false);
                getWriteNodeConnection(schema, autoCommit, handler, attachment, false);
            }
        }
    }

    public BackendConnection getConnection(String schema, boolean autoCommit, Boolean runOnSlave, Object attachment) throws Exception {
        if (runOnSlave == null) {
            PhysicalDatasource readSource = dbPool.getRWBalanceNode();
            if (!readSource.isAlive()) {
                String heartbeatError = "the data source[" + readSource.getConfig().getUrl() + "] can't reach. Please check the dataHost status";
                if (dbPool.getDataHostConfig().isShowSlaveSql()) {
                    heartbeatError += ",Tip:heartbeat[show slave status] need the SUPER or REPLICATION CLIENT privilege(s)";
                }
                LOGGER.warn(heartbeatError);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", readSource.getHostConfig().getName() + "-" + readSource.getConfig().getHostName());
                AlertUtil.alert(AlarmCode.DATA_HOST_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", readSource.getConfig().getId(), labels);
                throw new IOException(heartbeatError);
            }
            return readSource.getConnection(schema, autoCommit, attachment);
        } else if (runOnSlave) {
            PhysicalDatasource source = dbPool.getReadNode();
            return source.getConnection(schema, autoCommit, attachment);
        } else {
            checkRequest(schema);
            if (dbPool.isInitSuccess()) {
                PhysicalDatasource writeSource = dbPool.getSource();
                if (writeSource.isReadOnly()) {
                    throw new IllegalArgumentException("The Data Source[" + writeSource.getConfig().getUrl() + "] is running with the --read-only option so it cannot execute this statement");
                }
                writeSource.setWriteCount();
                return writeSource.getConnection(schema, autoCommit, attachment);
            } else {
                throw new IllegalArgumentException("Invalid DataSource:" + dbPool.hostName);
            }
        }
    }

    private void getWriteNodeConnection(String schema, boolean autoCommit, ResponseHandler handler, Object attachment, boolean fakeRead) throws IOException {
        checkRequest(schema);
        if (dbPool.isInitSuccess()) {
            PhysicalDatasource writeSource = dbPool.getSource();
            if (writeSource.isDisabled()) {
                throw new IllegalArgumentException("[" + writeSource.getHostConfig().getName() + "." + writeSource.getConfig().getHostName() + "] is disabled");
            }
            if (!fakeRead && writeSource.isReadOnly()) {
                throw new IllegalArgumentException("The Data Source[" + writeSource.getConfig().getUrl() + "] is running with the --read-only option so it cannot execute this statement");
            }
            writeSource.setWriteCount();
            writeSource.getConnection(schema, autoCommit, handler, attachment, true);
        } else {
            throw new IllegalArgumentException("Invalid DataSource:" + dbPool.hostName);
        }
    }
}
