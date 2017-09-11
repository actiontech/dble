/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.ConMap;
import com.actiontech.dble.backend.ConQueue;
import com.actiontech.dble.backend.heartbeat.DBHeartbeat;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import com.actiontech.dble.backend.mysql.nio.handler.DelegateResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.NewConnectionRespHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.Alarms;
import com.actiontech.dble.config.model.DBHostConfig;
import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PhysicalDatasource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);

    private final String name;
    private final int size;
    private final DBHostConfig config;
    private final ConMap conMap = new ConMap();
    private DBHeartbeat heartbeat;
    private final boolean readNode;
    private volatile long heartbeatRecoveryTime;
    private final DataHostConfig hostConfig;
    private PhysicalDBPool dbPool;
    private AtomicBoolean dying = new AtomicBoolean(false);

    private AtomicLong readCount = new AtomicLong(0);

    private AtomicLong writeCount = new AtomicLong(0);

    public PhysicalDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
        this.size = config.getMaxCon();
        this.config = config;
        this.name = config.getHostName();
        this.hostConfig = hostConfig;
        heartbeat = this.createHeartBeat();
        this.readNode = isReadNode;
    }

    public boolean isMyConnection(BackendConnection con) {
        if (con instanceof MySQLConnection) {
            return ((MySQLConnection) con).getPool() == this;
        } else {
            return false;
        }
    }

    public boolean getDying() {
        return dying.get();
    }

    public void setDying() {
        heartbeat.stop();
        dying.compareAndSet(false, true);
        closeByDyingAll();
    }

    public long getReadCount() {
        return readCount.get();
    }

    public void setReadCount() {
        readCount.addAndGet(1);
    }

    public long getWriteCount() {
        return writeCount.get();
    }

    public void setWriteCount() {
        writeCount.addAndGet(1);
    }

    public DataHostConfig getHostConfig() {
        return hostConfig;
    }

    public boolean isReadNode() {
        return readNode;
    }

    public int getSize() {
        return size;
    }

    public void setDbPool(PhysicalDBPool dbPool) {
        this.dbPool = dbPool;
    }

    public PhysicalDBPool getDbPool() {
        return dbPool;
    }

    public abstract DBHeartbeat createHeartBeat();

    public String getName() {
        return name;
    }

    public long getExecuteCount() {
        long executeCount = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            executeCount += queue.getExecuteCount();

        }
        return executeCount;
    }

    public long getExecuteCountForSchema(String schema) {
        return conMap.getSchemaConQueue(schema).getExecuteCount();

    }

    public int getActiveCountForSchema(String schema) {
        return conMap.getActiveCountForSchema(schema, this);
    }

    public int getIdleCountForSchema(String schema) {
        ConQueue queue = conMap.getSchemaConQueue(schema);
        int total = 0;
        total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        return total;
    }

    public DBHeartbeat getHeartbeat() {
        return heartbeat;
    }

    public int getIdleCount() {
        int total = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        }
        return total;
    }

    public boolean isSalveOrRead() {
        return dbPool.isSlave(this) || this.readNode;
    }

    public void connectionHeatBeatCheck(long conHeartBeatPeriod) {
        // to die
        if (dying.get()) {
            closeByDyingAll();
            return;
        }

        long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;

        for (ConQueue queue : conMap.getAllConQueue()) {
            longIdleHeartBeat(queue.getAutoCommitCons(), hearBeatTime);
            longIdleHeartBeat(queue.getManCommitCons(), hearBeatTime);
        }

        //the following is about the idle connection number control
        int idleCons = getIdleCount();
        int activeCons = this.getActiveCount();
        int createCount = (hostConfig.getMinCon() - idleCons) / 3;

        // create if idle too little
        if ((createCount > 0) && (idleCons + activeCons < size)) {
            createByIdleLitte(idleCons, createCount);
        } else if (idleCons > hostConfig.getMinCon()) {
            closeByIdleMany(idleCons - hostConfig.getMinCon(), idleCons);
        } else {
            int activeCount = this.getActiveCount();
            if (activeCount > size) {
                String s = Alarms.DEFAULT + "DATASOURCE EXCEED [name=" + name +
                        ",active=" + activeCount +
                        ",size=" + size + ']';
                LOGGER.warn(s);
            }
        }
    }


    /**
     * check if the connection is not be used for a while & do connection heart beat
     *
     * @param linkedQueue
     * @param hearBeatTime
     */
    private void longIdleHeartBeat(ConcurrentLinkedQueue<BackendConnection> linkedQueue, long hearBeatTime) {
        long length = linkedQueue.size();
        for (int i = 0; i < length; i++) {
            BackendConnection con = linkedQueue.poll();
            if (con.isClosedOrQuit()) {
                continue;
            } else if (con.getLastTime() < hearBeatTime) { //if the connection is idle for a long time
                con.setBorrowed(true);
                new ConnectionHeartBeatHandler().doHeartBeat(con);
            } else {
                linkedQueue.offer(con);
                break;
            }
        }
    }


    private void closeByDyingAll() {
        List<BackendConnection> readyCloseCons = new ArrayList<>(this.getIdleCount());

        for (ConQueue queue : conMap.getAllConQueue()) {
            readyCloseCons.addAll(queue.getIdleConsToClose());
        }

        for (BackendConnection idleCon : readyCloseCons) {
            if (idleCon != null) {
                if (idleCon.isBorrowed()) {
                    LOGGER.warn("find idle con is using " + idleCon);
                }
                idleCon.close("dying");
            } else {
                break;
            }
        }
        if (this.conMap.getActiveCountForDs(this) == 0) {
            this.dbPool.delRDs(this);
        }
    }

    private void closeByIdleMany(int ildeCloseCount, int idleCons) {
        LOGGER.info("too many ilde cons ,close some for datasouce  " + name + " want close :" + ildeCloseCount + " total idle " + idleCons);
        List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(ildeCloseCount);
        for (ConQueue queue : conMap.getAllConQueue()) {
            int closeNumber = (queue.getManCommitCons().size() + queue.getAutoCommitCons().size()) * ildeCloseCount / idleCons;
            readyCloseCons.addAll(queue.getIdleConsToClose(closeNumber));
        }

        for (BackendConnection idleCon : readyCloseCons) {
            if (idleCon.isBorrowed()) {
                LOGGER.warn("find idle con is using " + idleCon);
            }
            idleCon.close("too many idle con");
        }
    }

    private void createByIdleLitte(int idleCons, int createCount) {
        LOGGER.info("create connections ,because idle connection not enough ,cur is " +
                idleCons + ", minCon is " + hostConfig.getMinCon() + " for " + name);

        final String[] schemas = dbPool.getSchemas();
        for (int i = 0; i < createCount; i++) {
            if (this.getActiveCount() + this.getIdleCount() >= size) {
                break;
            }
            NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();
            try {
                // creat new connection
                this.createNewConnection(simpleHandler, null, schemas[i % schemas.length]);
                simpleHandler.getBackConn().release();
            } catch (IOException e) {
                LOGGER.warn("create connection err " + e);
            }
        }
    }

    public int getActiveCount() {
        return this.conMap.getActiveCountForDs(this);
    }

    public void clearCons(String reason) {
        this.conMap.clearConnections(reason, this);
    }

    public void clearConsByDying() {
        clearCons("smooth dying");
        this.dbPool.delRDs(this);
    }

    public void startHeartbeat() {
        heartbeat.start();
    }

    public void stopHeartbeat() {
        heartbeat.stop();
    }

    public void doHeartbeat() {
        if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
            return;
        }

        if (!heartbeat.isStop()) {
            try {
                heartbeat.heartbeat();
            } catch (Exception e) {
                LOGGER.error(name + " heartbeat error.", e);
            }
        }
    }

    private BackendConnection takeCon(BackendConnection conn, String schema) {
        conn.setBorrowed(true);
        if (!conn.getSchema().equals(schema)) {
            // need do schema syn in before sql send
            conn.setSchema(schema);
        }
        ConQueue queue = conMap.getSchemaConQueue(schema);
        queue.incExecuteCount();
        // update last time, the schedule job will not close it
        conn.setLastTime(System.currentTimeMillis());
        return conn;
    }

    private BackendConnection takeCon(BackendConnection conn,
                                      final ResponseHandler handler, final Object attachment,
                                      String schema) {
        takeCon(conn, schema);
        conn.setAttachment(attachment);
        handler.connectionAcquired(conn);
        return conn;
    }

    private void createNewConnection(final ResponseHandler handler, final Object attachment,
                                     final String schema) throws IOException {
        // aysn create connection
        DbleServer.getInstance().getBusinessExecutor().execute(new Runnable() {
            public void run() {
                try {
                    createNewConnection(new DelegateResponseHandler(handler) {
                        @Override
                        public void connectionError(Throwable e, BackendConnection conn) {
                            handler.connectionError(e, conn);
                        }

                        @Override
                        public void connectionAcquired(BackendConnection conn) {
                            takeCon(conn, handler, attachment, schema);
                        }
                    }, schema);
                } catch (IOException e) {
                    handler.connectionError(e, null);
                }
            }
        });
    }

    public abstract void createNewConnection(ResponseHandler handler, String schema) throws IOException;

    public void getConnection(String schema, boolean autocommit, final ResponseHandler handler,
                              final Object attachment) throws IOException {
        if (dying.get()) {
            closeByDyingAll();
            LOGGER.info(this.name + "will to die");
            throw new IOException(this.name + "will to die");
        }

        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
            takeCon(con, handler, attachment, schema);
            return;
        } else {
            int activeCons = this.getActiveCount();
            if (activeCons + 1 > size) {
                String msg = "the max activeConnnections size can not be max than maxconnections";
                LOGGER.error(msg);
                throw new IOException(msg);
            } else { // create connection
                LOGGER.info("no ilde connection in pool,create new connection for " +
                        this.name + " of schema " + schema);
                createNewConnection(handler, attachment, schema);
            }
        }
    }

    public BackendConnection getConnection(String schema, boolean autocommit) throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con == null) {
            int activeCons = this.getActiveCount(); // the max active
            if (activeCons + 1 > size) {
                LOGGER.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException("the max activeConnnections size can not be max than maxconnections");
            } else { // create connection
                LOGGER.info(
                        "no ilde connection in pool,create new connection for " + this.name + " of schema " + schema);
                NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();
                this.createNewConnection(simpleHandler, schema);
                con = simpleHandler.getBackConn();
            }
        }
        return takeCon(con, schema);
    }

    private void returnCon(BackendConnection c) {
        if (dying.get()) {
            c.close("dying");
            closeByDyingAll();
            return;
        }

        c.setAttachment(null);
        c.setBorrowed(false);
        c.setLastTime(TimeUtil.currentTimeMillis());
        ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

        boolean ok = false;
        if (c.isAutocommit()) {
            ok = queue.getAutoCommitCons().offer(c);
        } else {
            ok = queue.getManCommitCons().offer(c);
        }

        if (!ok) {

            LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
    }

    public void releaseChannel(BackendConnection c) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release channel " + c);
        }
        // release connection
        returnCon(c);
    }

    public void connectionClosed(BackendConnection conn) {
        ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
        if (queue != null) {
            queue.removeCon(conn);
        }
    }

    /**
     * used for init or reload
     */
    public abstract boolean testConnection(String schema) throws IOException;

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public DBHostConfig getConfig() {
        return config;
    }

    public boolean isAlive() {
        return (getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS) && !getDying();
    }
}
