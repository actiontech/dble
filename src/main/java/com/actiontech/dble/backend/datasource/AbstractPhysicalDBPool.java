package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by szf on 2019/10/18.
 */
public abstract class AbstractPhysicalDBPool {
    public static final int BALANCE_NONE = 0;
    protected static final int BALANCE_ALL_BACK = 1;
    protected static final int BALANCE_ALL = 2;

    public static final int WEIGHT = 0;

    protected final String hostName;
    protected final int balance;
    protected final DataHostConfig dataHostConfig;
    protected final ThreadLocalRandom random = ThreadLocalRandom.current();
    protected volatile boolean initSuccess = false;
    protected String[] schemas;

    protected PhysicalDatasource[] readSources;

    protected final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();


    protected AbstractPhysicalDBPool(String hostName, int balance, DataHostConfig config) {
        this.hostName = hostName;
        this.balance = balance;
        this.dataHostConfig = config;
    }

    /**
     * find the datasource of a exist backendConnection
     *
     * @param exitsCon
     * @return
     */
    abstract PhysicalDatasource findDatasource(BackendConnection exitsCon);

    abstract boolean isSlave(PhysicalDatasource ds);

    public abstract PhysicalDatasource getSource();

    public boolean isInitSuccess() {
        return initSuccess;
    }

    public abstract boolean init();

    public abstract void doHeartbeat();

    public abstract void heartbeatCheck(long ildCheckPeriod);

    public abstract void startHeartbeat();

    public abstract void stopHeartbeat();

    public abstract void clearDataSources(String reason);

    public abstract Collection<PhysicalDatasource> getAllActiveDataSources(); // not contains StandbyReadSources

    public abstract Collection<PhysicalDatasource> getAllDataSources();

    abstract void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception;

    abstract PhysicalDatasource getRWBalanceNode();

    abstract PhysicalDatasource getReadNode() throws Exception;

    abstract boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception;

    boolean equalsBaseInfo(AbstractPhysicalDBPool pool) {
        return pool.getDataHostConfig().getName().equals(this.dataHostConfig.getName()) &&
                pool.getDataHostConfig().getHearbeatSQL().equals(this.dataHostConfig.getHearbeatSQL()) &&
                pool.getDataHostConfig().getHeartbeatTimeout() == this.dataHostConfig.getHeartbeatTimeout() &&
                pool.getDataHostConfig().getErrorRetryCount() == this.dataHostConfig.getErrorRetryCount() &&
                pool.getDataHostConfig().getBalance() == this.dataHostConfig.getBalance() &&
                pool.getDataHostConfig().getMaxCon() == this.dataHostConfig.getMaxCon() &&
                pool.getDataHostConfig().getMinCon() == this.dataHostConfig.getMinCon() &&
                pool.getHostName().equals(this.hostName);
    }

    public String[] getSchemas() {
        return schemas;
    }

    public void setSchemas(String[] mySchemas) {
        this.schemas = mySchemas;
    }

    public DataHostConfig getDataHostConfig() {
        return dataHostConfig;
    }

    abstract PhysicalDatasource getWriteSource();

    public String getHostName() {
        return hostName;
    }

    public PhysicalDatasource randomSelect(ArrayList<PhysicalDatasource> okSources) {
        if (okSources.isEmpty()) {
            return this.getSource();
        } else {
            int length = okSources.size();
            int totalWeight = 0;
            boolean sameWeight = true;
            for (int i = 0; i < length; i++) {
                int weight = okSources.get(i).getConfig().getWeight();
                totalWeight += weight;
                if (sameWeight && i > 0 && weight != okSources.get(i - 1).getConfig().getWeight()) {
                    sameWeight = false;
                }
            }

            if (totalWeight > 0 && !sameWeight) {
                // random by different weight
                int offset = random.nextInt(totalWeight);
                for (PhysicalDatasource okSource : okSources) {
                    offset -= okSource.getConfig().getWeight();
                    if (offset < 0) {
                        return okSource;
                    }
                }
            }
            return okSources.get(random.nextInt(length));
        }
    }

    public abstract int next(int i);

    protected boolean checkSlaveSynStatus() {
        return (dataHostConfig.getSlaveThreshold() != -1) &&
                (dataHostConfig.isShowSlaveSql());
    }

    protected boolean canSelectAsReadNode(PhysicalDatasource theSource) {
        Integer slaveBehindMaster = theSource.getHeartbeat().getSlaveBehindMaster();
        int dbSynStatus = theSource.getHeartbeat().getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == MySQLHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == MySQLHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster < this.dataHostConfig.getSlaveThreshold();
        return isSync && isNotDelay;
    }

    public PhysicalDatasource[] getReadSources() {
        return this.readSources;
    }

}
