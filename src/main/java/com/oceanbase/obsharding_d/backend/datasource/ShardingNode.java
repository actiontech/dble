/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.datasource;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class ShardingNode {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ShardingNode.class);

    protected final String name;
    private final String dbGroupName;
    protected String database;
    protected volatile PhysicalDbGroup dbGroup;
    private volatile boolean isSchemaExists = false;

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

    public void syncGetConnection(String schema, boolean isMustWrite, boolean autoCommit, RouteResultsetNode rrs,
                                  ResponseHandler handler, Object attachment) throws Exception {

        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-sharding-node");
        try {
            checkRequest(schema);
            PhysicalDbInstance instance = dbGroup.select(canRunOnMaster(rrs, !isMustWrite && autoCommit), rrs.isForUpdate(), localRead(rrs.getSqlType()));
            instance.syncGetConnection(schema, handler, attachment, isMustWrite);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void getConnection(String schema, boolean isMustWrite, boolean autoCommit, RouteResultsetNode rrs,
                              ResponseHandler handler, Object attachment) throws Exception {

        TraceManager.TraceObject traceObject = TraceManager.threadTrace("get-connection-from-sharding-node");
        try {
            checkRequest(schema);
            PhysicalDbInstance instance = dbGroup.select(canRunOnMaster(rrs, !isMustWrite && autoCommit), rrs.isForUpdate(), localRead(rrs.getSqlType()));
            instance.getConnection(schema, handler, attachment, isMustWrite);
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public BackendConnection getConnection(String schema, boolean autocommit, Object attachment) throws IOException {
        checkRequest(schema);
        RouteResultsetNode rrs = (RouteResultsetNode) attachment;
        PhysicalDbInstance instance = dbGroup.select(canRunOnMaster(rrs, autocommit), rrs.isForUpdate(), localRead(rrs.getSqlType()));
        return instance.getConnection(schema, attachment);
    }

    // if force master,set canRunInReadDB=false
    // if force slave set runOnSlave,default null means not effect
    private Boolean canRunOnMaster(RouteResultsetNode rrs, boolean autoCommit) {
        Boolean master = null;
        if (rrs.getRunOnSlave() == null) {
            if (!rrs.canRunINReadDB(autoCommit)) {
                master = true;
            }
        } else {
            // force slave
            if (rrs.getRunOnSlave()) {
                master = false;
            } else {
                rrs.setCanRunInReadDB(false);
                master = true;
            }
        }
        return master;
    }

    private boolean localRead(int sqlType) {
        return sqlType == ServerParse.SELECT;
    }


    public boolean equalsBaseInfo(ShardingNode shardingNode) {
        return StringUtil.equalsWithEmpty(this.name, shardingNode.getName()) &&
                StringUtil.equalsWithEmpty(this.dbGroupName, shardingNode.getDbGroupName()) &&
                StringUtil.equalsWithEmpty(this.database, shardingNode.getDatabase()) &&
                this.dbGroup.equalsBaseInfo(shardingNode.getDbGroup());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardingNode that = (ShardingNode) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(dbGroupName, that.dbGroupName) &&
                Objects.equals(database, that.database);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dbGroupName, database);
    }
}
