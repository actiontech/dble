/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PhysicalDBNode {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBNode.class);

    protected final String name;
    protected final String database;
    protected final PhysicalDBPool dbPool;

    public PhysicalDBNode(String hostName, String database, PhysicalDBPool dbPool) {
        this.name = hostName;
        this.database = database;
        this.dbPool = dbPool;
    }

    public String getName() {
        return name;
    }

    public PhysicalDBPool getDbPool() {
        return dbPool;
    }

    public String getDatabase() {
        return database;
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
            throw new RuntimeException("can't find exits connection, maybe fininshed " + exitsCon);
        } else {
            ds.getConnection(schema, autocommit, handler, attachment);
        }
    }

    private void checkRequest(String schema) {
        if (schema != null && !schema.equals(this.database)) {
            throw new RuntimeException("invalid param ,connection request db is :" + schema +
                    " and datanode db is " + this.database);
        }
        if (!dbPool.isInitSuccess()) {
            int activeIndex = dbPool.init(dbPool.activeIndex);
            if (activeIndex >= 0) {
                DbleServer.getInstance().saveDataHostIndex(dbPool.getHostName(), activeIndex);
            } else {
                throw new RuntimeException(dbPool.getHostName() + " init source error ");
            }
        }
    }

    public void getConnection(String schema, boolean isMustWrite, boolean autoCommit, RouteResultsetNode rrs,
                              ResponseHandler handler, Object attachment) throws Exception {
        if (isMustWrite) {
            getWriteNodeConnection(schema, autoCommit, handler, attachment);
            return;
        }
        if (rrs.getRunOnSlave() == null) {
            if (rrs.canRunINReadDB(autoCommit)) {
                dbPool.getRWBalanceCon(schema, autoCommit, handler, attachment);
            } else {
                getWriteNodeConnection(schema, autoCommit, handler, attachment);
            }
        } else {
            if (rrs.getRunOnSlave()) {
                if (!dbPool.getReadCon(schema, autoCommit, handler, attachment)) {
                    LOGGER.info("Do not have slave connection to use, use master connection instead.");
                    rrs.setRunOnSlave(false);
                    rrs.setCanRunInReadDB(false);
                    getWriteNodeConnection(schema, autoCommit, handler, attachment);
                }
            } else {
                rrs.setCanRunInReadDB(false);
                getWriteNodeConnection(schema, autoCommit, handler, attachment);
            }
        }
    }

    public BackendConnection getConnection(String schema, boolean autoCommit, boolean canRunINReadDB) throws Exception {
        if (canRunINReadDB) {
            PhysicalDatasource readSource = dbPool.getRWBalanceNode();
            return readSource.getConnection(schema, autoCommit);
        } else {
            checkRequest(schema);
            if (dbPool.isInitSuccess()) {
                PhysicalDatasource writeSource = dbPool.getSource();
                writeSource.setWriteCount();
                return writeSource.getConnection(schema, autoCommit);
            } else {
                throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActiveIndex());
            }
        }
    }

    private void getWriteNodeConnection(String schema, boolean autoCommit, ResponseHandler handler, Object attachment) throws IOException {
        checkRequest(schema);
        if (dbPool.isInitSuccess()) {
            PhysicalDatasource writeSource = dbPool.getSource();
            writeSource.setWriteCount();
            writeSource.getConnection(schema, autoCommit, handler, attachment);
        } else {
            throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActiveIndex());
        }
    }
}
