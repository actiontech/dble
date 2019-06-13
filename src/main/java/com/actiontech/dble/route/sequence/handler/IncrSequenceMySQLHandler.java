/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.route.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.SQLNonTransientException;

public class IncrSequenceMySQLHandler implements SequenceHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceMySQLHandler.class);

    private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
    protected static final String ERR_SEQ_RESULT = "-999999999,null";
    protected static final Map<String, String> LATEST_ERRORS = new ConcurrentHashMap<>();
    private final FetchMySQLSequenceHandler mysqlSeqFetcher = new FetchMySQLSequenceHandler();
    private static final IncrSequenceMySQLHandler INSTANCE = new IncrSequenceMySQLHandler();
    private static Set<String> dataNodes = new HashSet<>();

    public static IncrSequenceMySQLHandler getInstance() {
        return IncrSequenceMySQLHandler.INSTANCE;
    }

    public void load(boolean isLowerCaseTableNames) {
        // load sequence properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS, isLowerCaseTableNames);
        removeDesertedSequenceVals(props);
        putNewSequenceVals(props);
    }

    public Set<String> getDataNodes() {
        return dataNodes;
    }

    private void removeDesertedSequenceVals(Properties props) {
        Iterator<Map.Entry<String, SequenceVal>> i = seqValueMap.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, SequenceVal> entry = i.next();
            if (!props.containsKey(entry.getKey())) {
                i.remove();
            }
        }
    }

    private void putNewSequenceVals(Properties props) {
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String seqName = (String) entry.getKey();
            String dataNode = (String) entry.getValue();
            SequenceVal value = seqValueMap.putIfAbsent(seqName, new SequenceVal(seqName, dataNode));
            if (value != null) {
                value.dataNode = dataNode;
            }
            dataNodes.add(dataNode);
        }
    }

    /**
     * save sequence -> curval
     */
    private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<>();

    @Override
    public long nextId(String seqName) throws SQLNonTransientException {
        SequenceVal seqVal = seqValueMap.get(seqName);
        if (seqVal == null) {
            throw new ConfigException("can't find definition for sequence :" + seqName);
        }
        if (!seqVal.isSuccessFetched()) {
            return getSeqValueFromDB(seqVal);
        } else {
            return getNextValidSeqVal(seqVal);
        }

    }

    private Long getNextValidSeqVal(SequenceVal seqVal) throws SQLNonTransientException {
        long nexVal = seqVal.counter.getNext();
        if (nexVal != -1) {
            return nexVal;
        } else {
            return getSeqValueFromDB(seqVal);
        }
    }

    private long getSeqValueFromDB(SequenceVal seqVal) throws SQLNonTransientException {
        if (seqVal.fetching.compareAndSet(false, true)) {
            //if get the lock ,connect to mysql and get next
            return this.execSeqFetcher(seqVal);
        } else {
            //other who does get the lock just wait for awhile
            return this.waitForResult(seqVal);
        }
    }

    /**
     * get the next segment & get the value[0]
     *
     * @param seqVal
     * @return
     * @throws SQLNonTransientException
     */
    private long execSeqFetcher(SequenceVal seqVal) throws SQLNonTransientException {
        try {
            seqVal.dbretVal = null;
            seqVal.dbfinished = false;
            mysqlSeqFetcher.execute(seqVal);
            Long[] values = seqVal.waitFinish();

            //check if the result is right
            if (values == null) {
                throw new RuntimeException("can't fetch sequence in db,sequence :" + seqVal.seqName + " detail:" +
                        mysqlSeqFetcher.getLastError(seqVal.seqName));
            } else if (values[0] == 0) {
                String msg = "sequence," + seqVal.seqName + "has not been set, please check configure in dble_sequence";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            } else {
                //if the result is OK just return the first value
                seqVal.setNewCounter(values[0], values[1]);
                return values[0];
            }
        } catch (Exception e) {
            throw e;
        } finally {
            seqVal.signalAll();
        }
    }

    /**
     * waiting for the packet exec to finish and get the next value
     *
     * @param seqVal
     * @return
     * @throws SQLNonTransientException
     */
    private long waitForResult(SequenceVal seqVal) throws SQLNonTransientException {
        seqVal.waitOtherFinish();
        return this.getNextValidSeqVal(seqVal);
    }

}




