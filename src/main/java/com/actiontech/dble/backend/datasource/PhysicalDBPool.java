/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.datasource;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.backend.mysql.nio.handler.GetConnectionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DataHostConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class PhysicalDBPool extends AbstractPhysicalDBPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBPool.class);
    private PhysicalDatasource[] writeSources;
    private Collection<PhysicalDatasource> allActiveDs;
    private Collection<PhysicalDatasource> allSources;

    volatile int activeIndex;
    private final ReentrantLock switchLock = new ReentrantLock();

    public PhysicalDBPool(String name, DataHostConfig conf, PhysicalDatasource[] writeSources,
                          Map<Integer, PhysicalDatasource[]> readSources,
                          Map<Integer, PhysicalDatasource[]> standbyReadSourcesMap, int balance) {
        super(name, balance, conf);
        this.writeSources = writeSources;
        this.readSources = readSources;
        this.standbyReadSourcesMap = standbyReadSourcesMap;
        this.allActiveDs = this.genAllActiveDataSources();
        this.allSources = this.genAllDataSources();
        LOGGER.info("total resources of dataHost " + this.hostName + " is :" + allActiveDs.size());

        setDataSourceProps();
    }

    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allActiveDs) {
            ds.setDbPool(this);
        }
    }

    private Collection<PhysicalDatasource> genAllActiveDataSources() {
        LinkedList<PhysicalDatasource> allActiveSources = new LinkedList<>();
        for (PhysicalDatasource ds : writeSources) {
            if (ds != null) {
                allActiveSources.add(ds);
            }
        }

        for (PhysicalDatasource[] dataSources : this.readSources.values()) {
            for (PhysicalDatasource ds : dataSources) {
                if (ds != null) {
                    allActiveSources.add(ds);
                }
            }
        }
        return allActiveSources;
    }

    private Collection<PhysicalDatasource> genAllDataSources() {
        LinkedList<PhysicalDatasource> allDataSources = new LinkedList<>();
        allDataSources.addAll(this.allActiveDs);
        for (PhysicalDatasource[] standbyReadSources : standbyReadSourcesMap.values()) {
            allDataSources.addAll(Arrays.asList(standbyReadSources));
        }
        return allDataSources;
    }

    PhysicalDatasource findDatasource(BackendConnection exitsCon) {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allActiveDs;
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

    public String getHostName() {
        return hostName;
    }

    /* all write data nodes */
    public PhysicalDatasource[] getSources() {
        return writeSources;
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
                curDsHbStatus != MySQLHeartbeat.OK_STATUS && getSources().length > 1) {
            // try to see if need switch datasource
            if (curDsHbStatus != MySQLHeartbeat.INIT_STATUS) {
                int curIndex = getActiveIndex();
                int nextId = next(curIndex);
                PhysicalDatasource[] allWriteNodes = getSources();
                while (true) {
                    if (nextId == curIndex) {
                        break;
                    }

                    PhysicalDatasource theDs = allWriteNodes[nextId];
                    MySQLHeartbeat theDsHb = theDs.getHeartbeat();
                    if (theDsHb.getStatus() == MySQLHeartbeat.OK_STATUS) {
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
                            LOGGER.warn("try to switch datasource ,not checked slave synchronize status " +
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
                int result = this.innerInit(activeIndex);
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
        return innerInit(index);
    }

    private int innerInit(int index) {
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

    public void reloadInit(int index) {
        if (initSuccess) {
            LOGGER.info(hostName + "dataHost already inited doing nothing");
            return;
        }
        innerInit(index);
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
            LOGGER.warn("minCon size is less than (the count of schema +1), so dble will create at least 1 conn for every schema and an empty schema conn, " +
                    "minCon size before:{}, now:{}", ds.getConfig().getMinCon(), initSize);
            ds.getConfig().setMinCon(initSize);
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
            if (ds.getTotalConCount() <= 0) {
                ds.initMinConnection(null, true, getConHandler, null);
            } else {
                LOGGER.info("connection with null schema has been created,because we tested the connection of the datasource at first");
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
            all = this.allActiveDs;
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
        for (PhysicalDatasource[] physicalDatasource : standbyReadSourcesMap.values()) {
            for (PhysicalDatasource ds : physicalDatasource) {
                ds.doHeartbeat();
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
            all = this.allActiveDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource ds : all) {
            // only read node or all write node
            // and current write node will check
            if (ds != null && (ds.getHeartbeat().getStatus() == MySQLHeartbeat.OK_STATUS) &&
                    (ds.isReadNode() || ds == this.getSource())) {
                ds.connectionHeatBeatCheck(ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allActiveDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource source : all) {
            source.startHeartbeat();
        }
        for (PhysicalDatasource[] physicalDatasource : standbyReadSourcesMap.values()) {
            for (PhysicalDatasource ds : physicalDatasource) {
                ds.startHeartbeat();
            }
        }
    }

    public void stopHeartbeat() {
        Collection<PhysicalDatasource> all;
        adjustLock.readLock().lock();
        try {
            all = this.allActiveDs;
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
            all = this.allActiveDs;
        } finally {
            adjustLock.readLock().unlock();
        }

        for (PhysicalDatasource source : all) {
            LOGGER.info("clear datasource of pool  " + this.hostName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    public Collection<PhysicalDatasource> getAllActiveDataSources() {
        return this.allActiveDs;
    }

    public Collection<PhysicalDatasource> getAllDataSources() {
        return this.allSources;
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
        if (theNode.isDisabled()) {
            if (this.getReadSources().values().size() > 0) {
                theNode = this.getReadSources().values().iterator().next()[0];
            } else {
                String errorMsg = "the dataHost[" + theNode.getHostConfig().getName() + "] is disabled, please check it";
                throw new IOException(errorMsg);
            }
        }
        if (!theNode.isAlive()) {
            String heartbeatError = "the data source[" + theNode.getConfig().getUrl() + "] can't reach. Please check the dataHost status";
            if (dataHostConfig.isShowSlaveSql()) {
                heartbeatError += ",Tip:heartbeat[show slave status] need the SUPER or REPLICATION CLIENT privilege(s)";
            }
            LOGGER.warn(heartbeatError);
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", theNode.getHostConfig().getName() + "-" + theNode.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.DATA_HOST_CAN_NOT_REACH, Alert.AlertLevel.WARN, heartbeatError, "mysql", theNode.getConfig().getId(), labels);
            throw new IOException(heartbeatError);
        }
        theNode.getConnection(schema, autocommit, handler, attachment, false);
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
                theNode.getConnection(schema, autocommit, handler, attachment, false);
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

    private boolean isAlive(PhysicalDatasource theSource) {
        return theSource.isAlive();
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
            all = this.allActiveDs;
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


    public String toString() {
        StringBuilder sb = new StringBuilder("dataHost:").append(hostName).append(this.hashCode());
        sb.append(" Max = ").append(dataHostConfig.getMaxCon()).append(" Min = ").append(this.dataHostConfig.getMinCon());
        for (int i = 0; i < writeSources.length; i++) {
            PhysicalDatasource writeHost = writeSources[0];
            sb.append("t\t\t writeHost").append(i).append(" url=").append(writeHost.getConfig().getUrl());
            PhysicalDatasource[] readSource = readSources.get(i);
            if (readSource != null) {
                for (PhysicalDatasource read : readSource) {
                    sb.append("t\t\t\t\t readHost").append(i).append(" url=").append(read.getConfig().getUrl());
                }
            }

        }

        return sb.toString();
    }

    public String[] getSchemas() {
        return schemas;
    }


    public PhysicalDatasource[] getWriteSources() {
        return writeSources;
    }

}
