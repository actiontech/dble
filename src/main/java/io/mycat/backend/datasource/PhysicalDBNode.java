/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.datasource;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        PhysicalDatasource ds = this.dbPool.findDatasouce(exitsCon);
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
                MycatServer.getInstance().saveDataHostIndex(dbPool.getHostName(), activeIndex);
            } else {
                throw new RuntimeException(dbPool.getHostName() + " init source error ");
            }
        }
    }

    public void getConnection(String schema, boolean autoCommit, RouteResultsetNode rrs,
                              ResponseHandler handler, Object attachment) throws Exception {
        checkRequest(schema);
        if (dbPool.isInitSuccess()) {
            LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
            if (rrs.getRunOnSlave() != null) {  // hint like /*db_type=master/slave*/
                // the hint is slave
                if (rrs.getRunOnSlave()) {
                    LOGGER.debug("rrs.isHasBlanceFlag() " + rrs.isHasBlanceFlag());
                    if (rrs.isHasBlanceFlag()) {  // hint like /*balance*/ (only support one?)
                        dbPool.getReadBalanceCon(schema, autoCommit, handler,
                                attachment);
                    } else {    // without /*balance*/
                        LOGGER.debug("rrs.isHasBlanceFlag()" + rrs.isHasBlanceFlag());
                        if (!dbPool.getReadCon(schema, autoCommit, handler,
                                attachment)) {
                            LOGGER.warn("Do not have slave connection to use, " +
                                    "use master connection instead.");
                            PhysicalDatasource writeSource = dbPool.getSource();
                            writeSource.setWriteCount();
                            writeSource.getConnection(schema, autoCommit,
                                    handler, attachment);
                            rrs.setRunOnSlave(false);
                            rrs.setCanRunInReadDB(false);
                        }
                    }
                } else {
                    LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
                    PhysicalDatasource writeSource = dbPool.getSource();
                    writeSource.setReadCount();
                    writeSource.getConnection(schema, autoCommit, handler, attachment);
                    rrs.setCanRunInReadDB(false);
                }
            } else {    // without hint like /*db_type=master/slave*/
                LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
                if (rrs.canRunnINReadDB(autoCommit)) {
                    dbPool.getRWBanlanceCon(schema, autoCommit, handler, attachment);
                } else {
                    PhysicalDatasource writeSource = dbPool.getSource();
                    writeSource.setWriteCount();
                    writeSource.getConnection(schema, autoCommit, handler, attachment);
                }
            }

        } else {
            throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActiveIndex());
        }
    }

    public BackendConnection getConnection(String schema, boolean autoCommit) throws Exception {
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
