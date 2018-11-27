/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.DBHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.GetConnectionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PhysicalDBPool {

    protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBPool.class);

    public static final int BALANCE_NONE = 0;
    private static final int BALANCE_ALL_BACK = 1;
    private static final int BALANCE_ALL = 2;
    private static final int BALANCE_ALL_READ = 3;

    public static final int WEIGHT = 0;

    private final String hostName;

    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();

    private PhysicalDatasource[] writeSources;
    private Map<Integer, PhysicalDatasource[]> readSources;
    private Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap;
    private Collection<PhysicalDatasource> allDs;

    volatile int activeIndex;
    private volatile boolean initSuccess;

    private final ReentrantLock switchLock = new ReentrantLock();

    private final int balance;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private String[] schemas;

    private final DataHostConfig dataHostConfig;

    public PhysicalDBPool(String name, DataHostConfig conf, PhysicalDatasource[] writeSources,
                          Map<Integer, PhysicalDatasource[]> readSources,
                          Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap, int balance) {
        this.hostName = name;
        this.dataHostConfig = conf;
        this.writeSources = writeSources;
        this.balance = balance;
        this.readSources = readSources;
        this.standbyReadSourcesMap = standbyReadSourcesMap;
        this.allDs = this.genAllDataSources();

        LOGGER.info("total resources of dataHost " + this.hostName + " is :" + allDs.size());

        setDataSourceProps();
    }

    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allDs) {
            ds.setDbPool(this);
        }
    }

    private Collection<PhysicalDatasource> genAllDataSources() {
        LinkedList<PhysicalDatasource> allSources = new LinkedList<>();
        for (PhysicalDatasource ds : writeSources) {
            if (ds != null) {
                allSources.add(ds);
            }
        }

        for (PhysicalDatasource[] dataSources : this.readSources.values()) {
            for (PhysicalDatasource ds : dataSources) {
                if (ds != null) {
                    allSources.add(ds);
                }
            }
        }
        return allSources;
    }

    PhysicalDatasource findDatasource(BackendConnection exitsCon) {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        // mgj: why not only isMyConnection??? from slave db is from read node.
        // switch change the read node???
        for (PhysicalDatasource ds : all) {
            if ((ds.isReadNode() == exitsCon.isFromSlaveDB()) && ds.isMyConnection(exitsCon)) {
                return ds;
            }
        }

        LOGGER.info("can't find connection in pool " + this.hostName + " con:" + exitsCon);
        return null;
    }

    // ensure never be invocated concurrently
    void delRDs(PhysicalDatasource source) {
        int index = -1;
        PhysicalDatasource[] nrDs = null;
        boolean del = false;

        for (Map.Entry<Integer, PhysicalDatasource[]> entry : readSources.entrySet()) {
            for (PhysicalDatasource ds : entry.getValue()) {
                if (ds == source) {
                    index = entry.getKey();
                    break;
                }
            }
        }

        PhysicalDatasource[] rDs = this.readSources.get(index);
        if (rDs.length == 1) {
            del = true;
        } else {
            nrDs = new PhysicalDatasource[rDs.length - 1];
            int i = 0;
            for (PhysicalDatasource ds : rDs) {
                if (ds != source) {
                    nrDs[i++] = ds;
                }
            }
        }

        adjustLock.writeLock().lock();
        try {
            if (del) {
                this.readSources.remove(index);
            } else {
                this.readSources.put(index, nrDs);
            }
            this.allDs = this.genAllDataSources();
        } finally {
            adjustLock.writeLock().unlock();
        }
    }

    // ensure never be invocated concurrently
    public void addRDs(int index, PhysicalDatasource source) {
        PhysicalDatasource[] nrDs;

        PhysicalDatasource[] rDs = this.readSources.get(index);
        if (rDs == null) {
            nrDs = new PhysicalDatasource[1];
            nrDs[0] = source;
        } else {
            nrDs = new PhysicalDatasource[rDs.length + 1];
            int i = 0;
            nrDs[i++] = source;
            for (PhysicalDatasource ds : rDs) {
                nrDs[i++] = ds;
            }
        }

        adjustLock.writeLock().lock();
        try {
            this.readSources.put(index, nrDs);
            this.allDs = this.genAllDataSources();
            source.setDbPool(this);
        } finally {
            adjustLock.writeLock().unlock();
        }
    }

    public String getHostName() {
        return hostName;
    }

    /* all write data nodes */
    public PhysicalDatasource[] getSources() {
        return writeSources;
    }

    public Map<Integer, PhysicalDatasource[]> getrReadSources() {
        return readSources;
    }

    public PhysicalDatasource getSource() {
        return writeSources[activeIndex];
    }

    boolean isSlave(PhysicalDatasource ds) {
        int currentIndex = 0;
        // mgj: immediately get the active ds and compare is good???
        for (int i = 0; i < getSources().length; i++) {
            PhysicalDatasource writeHostDatasource = getSources()[i];
            if (writeHostDatasource.getName().equals(ds.getName())) {
                currentIndex = i;
                break;
            }
        }
        return (currentIndex != activeIndex);
    }

    public boolean isInitSuccess() {
        return initSuccess;
    }

    public int getActiveIndex() {
        return activeIndex;
    }

    public int next(int i) {
        if (checkIndex(i)) {
            return (++i == writeSources.length) ? 0 : i;
        } else {
            return 0;
        }
    }

    public synchronized void switchSourceIfNeed(PhysicalDatasource ds, String reason) {
        int switchType = ds.getHostConfig().getSwitchType();
        int curDsHbStatus = getSource().getHeartbeat().getStatus();
        // read node can't switch, only write node can switch
        if (!ds.isReadNode() &&
                curDsHbStatus != DBHeartbeat.OK_STATUS && getSources().length > 1) {
            // try to see if need switch datasource
            if (curDsHbStatus != DBHeartbeat.INIT_STATUS) {
                int curIndex = getActiveIndex();
                int nextId = next(curIndex);
                PhysicalDatasource[] allWriteNodes = getSources();
                while (true) {
                    if (nextId == curIndex) {
                        break;
                    }

                    PhysicalDatasource theDs = allWriteNodes[nextId];
                    DBHeartbeat theDsHb = theDs.getHeartbeat();
                    if (theDsHb.getStatus() == DBHeartbeat.OK_STATUS) {
                        if (switchType == DataHostConfig.SYN_STATUS_SWITCH_DS) {
                            if (Integer.valueOf(0).equals(theDsHb.getSlaveBehindMaster())) {
                                LOGGER.warn("try to switch datasource, slave is " + "synchronized to master " + theDs.getConfig());
                                switchSource(nextId, reason);
                                break;
                            } else {
                                LOGGER.info("ignored  datasource ,slave is not synchronized to master, slave behind master :" + theDsHb.getSlaveBehindMaster() + " " + theDs.getConfig());
                            }
                        } else {
                            // normal switch
                            LOGGER.warn("try to switch datasource ,not checked slave" + "synchronize status " +
                                    theDs.getConfig());
                            switchSource(nextId, reason);
                            break;
                        }
                    }
                    nextId = next(nextId);
                }
            }
        }
    }

    public boolean switchSource(int newIndex, String reason) {
        if (!checkIndex(newIndex)) {
            return false;
        }
        final ReentrantLock lock = this.switchLock;
        lock.lock();
        try {
            int current = activeIndex;
            if (current != newIndex) {
                // switch index
                activeIndex = newIndex;
                // init again
                int result = this.init(activeIndex);
                if (result >= 0) {
                    DbleServer.getInstance().saveDataHostIndex(hostName, result, false);
                    // clear all connections
                    this.getSources()[current].clearCons("switch datasource");
                    // write log
                    LOGGER.warn(switchMessage(current, result, reason));
                    return true;
                } else {
                    LOGGER.warn(switchMessage(current, newIndex, reason) + ", but failed");
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    private String switchMessage(int current, int newIndex, String reason) {
        StringBuilder s = new StringBuilder();
        s.append("[Host=").append(hostName).append(",result=[").append(current).append("->");
        s.append(newIndex).append("],reason=").append(reason).append(']');
        return s.toString();
    }

    private int loop(int i) {
        return i < writeSources.length ? i : (i - writeSources.length);
    }

    public int init(int index) {
        if (!checkIndex(index)) {
            index = 0;
        }

        for (int i = 0; i < writeSources.length; i++) {
            int j = loop(i + index);
            if (initSource(j, writeSources[j])) {
                activeIndex = j;
                initSuccess = true;
                LOGGER.info(getMessage(j, " init success"));
                return activeIndex;
            }
        }

        initSuccess = false;
        LOGGER.warn(hostName + " init failure");
        return -1;
    }

    public int reloadInit(int index) {
        if (initSuccess) {
            LOGGER.info(hostName + "dataHost already inited doing nothing");
            return activeIndex;
        }
        return init(index);
    }

    private boolean checkIndex(int i) {
        return i >= 0 && i < writeSources.length;
    }

    private String getMessage(int index, String info) {
        return hostName + " index:" + index + info;
    }

    private boolean initSource(int index, PhysicalDatasource ds) {
        if (ds.getConfig().isDisabled()) {
            LOGGER.info(ds.getConfig().getHostName() + " is disabled, skipped");
            return true;
        }
        int initSize = ds.getConfig().getMinCon();
        if (initSize < this.schemas.length + 1) {
            initSize = this.schemas.length + 1;
            LOGGER.warn("minCon size is less than (the count of schema +1), so dble will create at least 1 conn for every schema and an empty schema conn");
        }

        if (ds.getConfig().getMaxCon() < initSize) {
            ds.getConfig().setMaxCon(initSize);
            ds.setSize(initSize);
            LOGGER.warn("maxCon is less than the initSize of dataHost:" + initSize + " change the maxCon into " + initSize);
        }

        LOGGER.info("init backend mysql source ,create connections total " + initSize + " for " + ds.getName() +
                " index :" + index);

        CopyOnWriteArrayList<BackendConnection> list = new CopyOnWriteArrayList<>();
        GetConnectionHandler getConHandler = new GetConnectionHandler(list, initSize);
        // long start = System.currentTimeMillis();
        // long timeOut = start + 5000 * 1000L;
        boolean hasConnectionInPool = false;
        try {
            if (ds.getActiveCount() <= 0) {
                ds.initMinConnection(null, true, getConHandler, null);
            } else {
                LOGGER.info("connection with null schema do not create,because testConnection in pool");
                getConHandler.initIncrement();
                hasConnectionInPool = true;
            }
        } catch (Exception e) {
            LOGGER.warn("init connection with schema null error", e);
        }

        for (int i = 0; i < initSize - 1; i++) {
            try {
                ds.initMinConnection(this.schemas[i % schemas.length], true, getConHandler, null);
            } catch (Exception e) {
                LOGGER.warn(getMessage(index, " init connection error."), e);
            }
        }


        long timeOut = System.currentTimeMillis() + 60 * 1000;

        // waiting for finish
        while (!getConHandler.finished() && (System.currentTimeMillis() < timeOut)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                /*
                 * hardly triggered no error is needed
                 */
                LOGGER.info("initError", e);
            }
        }
        LOGGER.info("init result :" + getConHandler.getStatusInfo());
        return !list.isEmpty() || hasConnectionInPool;
    }

    public void doHeartbeat() {
        if (writeSources == null || writeSources.length == 0) {
            return;
        }

        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource source : all) {
            if (source != null) {
                source.doHeartbeat();
            } else {
                LOGGER.warn(hostName + " current dataSource is null!");
            }
        }
    }

    /**
     * back physical connection heartbeat check
     */
    public void heartbeatCheck(long ildCheckPeriod) {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource ds : all) {
            // only read node or all write node
            // and current write node will check
            if (ds != null && (ds.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS) &&
                    (ds.isReadNode() || ds == this.getSource())) {
                ds.connectionHeatBeatCheck(ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource source : all) {
            source.startHeartbeat();
        }
    }

    public void stopHeartbeat() {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }
        for (PhysicalDatasource source : all) {
            source.stopHeartbeat();
        }
    }

    /**
     * clearDataSources
     *
     * @param reason reason
     */
    public void clearDataSources(String reason) {
        LOGGER.info("clear data sources of pool " + this.hostName);
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource source : all) {
            LOGGER.info("clear datasource of pool  " + this.hostName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    public Map<Integer, PhysicalDatasource[]> getReadSources() {
        adjustLock.readLock().lock();
        try {
            return this.readSources;
        } finally {
            adjustLock.readLock().unlock();
        }
    }

    public Collection<PhysicalDatasource> getAllDataSources() {
        return this.allDs;
    }


    public Map<Integer, PhysicalDatasource[]> getStandbyReadSourcesMap() {
        return standbyReadSourcesMap;
    }

    /**
     * return connection for read balance
     *
     * @param schema     schema
     * @param autocommit autocommit
     * @param handler    handler
     * @param attachment attachment
     * @throws Exception Exception
     */
    void getRWBalanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {
        PhysicalDatasource theNode = getRWBalanceNode();
        if (theNode.getConfig().isDisabled()) {
            if (this.getReadSources().values().size() > 0) {
                theNode = this.getReadSources().values().iterator().next()[0];
            } else {
                String errorMsg = "the dataHost[" + theNode.getHostConfig().getName() + "] is disabled, please check it";
                throw new IOException(errorMsg);
            }
        }
        if (!theNode.isAlive()) {
            String heartbeatError = "the data source[" + theNode.getConfig().getUrl() + "] can't reached, please check the dataHost";
            LOGGER.warn(heartbeatError);
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", theNode.getHostConfig().getName() + "-" + theNode.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.DATA_HOST_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", theNode.getConfig().getId(), labels);
            throw new IOException(heartbeatError);
        }
        theNode.getConnection(schema, autocommit, handler, attachment);
    }

    PhysicalDatasource getRWBalanceNode() {
        PhysicalDatasource theNode;
        ArrayList<PhysicalDatasource> okSources;
        switch (balance) {
            case BALANCE_ALL_BACK: {
                // all read nodes and the stand by masters
                okSources = getAllActiveRWSources(true, false, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL: {
                okSources = getAllActiveRWSources(true, true, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL_READ: {
                okSources = getAllActiveRWSources(false, false, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_NONE:
            default:
                // return default write data source
                theNode = this.getSource();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read source " + theNode.getName() + " for dataHost:" + this.getHostName());
        }
        theNode.setReadCount();
        return theNode;
    }


    PhysicalDatasource getReadNode() throws Exception {

        PhysicalDatasource theNode = null;
        Map<Integer, PhysicalDatasource[]> rs;
        adjustLock.readLock().lock();
        try {
            rs = this.readSources;
        } finally {
            adjustLock.readLock().unlock();
        }

        LOGGER.debug("!readSources.isEmpty() " + !rs.isEmpty());
        if (!rs.isEmpty()) {
            /* we try readSources.size() times */
            for (int j = 0; j < rs.size(); j++) {
                int index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % rs.size();
                PhysicalDatasource[] allSlaves = rs.get(index);

                if (allSlaves != null) {
                    index = Math.abs(random.nextInt(Integer.MAX_VALUE)) % allSlaves.length;
                    PhysicalDatasource slave = allSlaves[index];

                    if (isAlive(slave)) {
                        if (checkSlaveSynStatus()) {
                            if (canSelectAsReadNode(slave)) {
                                theNode = slave;
                                break;
                            }
                        } else {
                            theNode = slave;
                            break;
                        }
                    }
                }
            }
        }
        return theNode;
    }

    /**
     * get a random readHost connection from writeHost, used by slave hint
     *
     * @param schema     schema
     * @param autocommit autocommit
     * @param handler    handler
     * @param attachment attachment
     * @throws Exception Exception
     */
    boolean getReadCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment) throws Exception {

        PhysicalDatasource theNode = null;
        Map<Integer, PhysicalDatasource[]> rs;
        adjustLock.readLock().lock();
        try {
            rs = this.readSources;
        } finally {
            adjustLock.readLock().unlock();
        }

        LOGGER.debug("!readSources.isEmpty() " + !rs.isEmpty());
        if (!rs.isEmpty()) {
            theNode = getReadNode();
            if (theNode != null) {
                theNode.setReadCount();
                theNode.getConnection(schema, autocommit, handler, attachment);
                return true;
            } else {
                LOGGER.info("read host is not available.");
                return false;
            }
        } else {
            LOGGER.info("read host is empty, readSources is empty.");
            return false;
        }
    }

    private boolean checkSlaveSynStatus() {
        return (dataHostConfig.getSlaveThreshold() != -1) &&
                (dataHostConfig.isShowSlaveSql());
    }

    /**
     * <p>
     * randomSelect by weight
     *
     * @param okSources okSources
     * @return PhysicalDatasource
     */
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

            // sameWeight or all zero then random
            return okSources.get(random.nextInt(length));
            // int index = Math.abs(random.nextInt()) % okSources.size();
            // return okSources.get(index);
        }
    }

    private boolean isAlive(PhysicalDatasource theSource) {
        return theSource.isAlive();
    }

    private boolean canSelectAsReadNode(PhysicalDatasource theSource) {
        Integer slaveBehindMaster = theSource.getHeartbeat().getSlaveBehindMaster();
        int dbSynStatus = theSource.getHeartbeat().getDbSynStatus();
        if (slaveBehindMaster == null || dbSynStatus == DBHeartbeat.DB_SYN_ERROR) {
            return false;
        }
        boolean isSync = dbSynStatus == DBHeartbeat.DB_SYN_NORMAL;
        boolean isNotDelay = slaveBehindMaster < this.dataHostConfig.getSlaveThreshold();
        return isSync && isNotDelay;
    }

    /**
     * return all backup write sources
     *
     * @param includeWriteNode         if include write nodes
     * @param includeCurWriteNode      if include current active write node. invalid when <code>includeWriteNode<code> is false
     * @param filterWithSlaveThreshold filterWithSlaveThreshold
     * @return PhysicalDatasource list
     */
    private ArrayList<PhysicalDatasource> getAllActiveRWSources(boolean includeWriteNode, boolean includeCurWriteNode,
                                                                boolean filterWithSlaveThreshold) {
        Collection<PhysicalDatasource> all;
        Map<Integer, PhysicalDatasource[]> rs;
        adjustLock.readLock().lock();
        try {
            all = this.allDs;
            rs = this.readSources;
        } finally {
            adjustLock.readLock().unlock();
        }

        ArrayList<PhysicalDatasource> okSources = new ArrayList<>(all.size());

        for (int i = 0; i < this.writeSources.length; i++) {
            PhysicalDatasource theSource = writeSources[i];
            boolean isCurWriteNode = (i == activeIndex);
            if (isAlive(theSource)) {
                if (isCurWriteNode) { // write node is active node
                    if (includeWriteNode && includeCurWriteNode) {
                        okSources.add(theSource);
                    }
                    addReadSource(filterWithSlaveThreshold, rs, okSources, i);
                } else {
                    if (filterWithSlaveThreshold && theSource.isSalveOrRead()) {
                        boolean selected = canSelectAsReadNode(theSource);
                        if (!selected) {
                            continue; //if standby write host delay, all standby write host's slave will not be included
                        } else if (includeWriteNode) {
                            okSources.add(theSource);
                        }
                    }
                    addReadSource(filterWithSlaveThreshold, rs, okSources, i);
                }
            } else {
                if (isCurWriteNode && this.dataHostConfig.isTempReadHostAvailable()) {
                    addReadSource(filterWithSlaveThreshold, rs, okSources, i);
                } //if standby write host not alive ,all it's slave will not be included
            }
        }
        return okSources;
    }

    private void addReadSource(boolean filterWithSlaveThreshold, Map<Integer, PhysicalDatasource[]> rs, ArrayList<PhysicalDatasource> okSources, int i) {
        if (!rs.isEmpty()) {
            // check all slave nodes
            PhysicalDatasource[] allSlaves = rs.get(i);
            if (allSlaves != null) {
                for (PhysicalDatasource slave : allSlaves) {
                    if (isAlive(slave) && (!filterWithSlaveThreshold || canSelectAsReadNode(slave))) {
                        okSources.add(slave);
                    }
                }
            }
        }
    }

    public boolean equalsBaseInfo(PhysicalDBPool pool) {

        if (pool.getDataHostConfig().getName().equals(this.dataHostConfig.getName()) &&
                pool.getDataHostConfig().getHearbeatSQL().equals(this.dataHostConfig.getHearbeatSQL()) &&
                pool.getDataHostConfig().getBalance() == this.dataHostConfig.getBalance() &&
                pool.getDataHostConfig().getMaxCon() == this.dataHostConfig.getMaxCon() &&
                pool.getDataHostConfig().getMinCon() == this.dataHostConfig.getMinCon() &&
                pool.getHostName().equals(this.hostName)) {
            return true;
        }
        return false;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer("dataHost:").append(hostName).append(this.hashCode());
        sb.append(" Max = ").append(dataHostConfig.getMaxCon()).append(" Min = ").append(this.dataHostConfig.getMinCon());
        for (int i = 0; i < writeSources.length; i++) {
            PhysicalDatasource writeHost = writeSources[0];
            sb.append("\n\t\t\t writeHost" + i).append(" url=").append(writeHost.getConfig().getUrl());
            PhysicalDatasource[] readSource = readSources.get(Integer.valueOf(i));
            if (readSource != null) {
                for (PhysicalDatasource read : readSource) {
                    sb.append("\n\t\t\t\t\t readHost" + i).append(" url=").append(read.getConfig().getUrl());
                }
            }

        }

        return sb.toString();
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


    public PhysicalDatasource[] getWriteSources() {
        return writeSources;
    }

}
