/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.route.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class IncrSequenceMySQLHandler implements SequenceHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceMySQLHandler.class);

    private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
    protected static final String ERR_SEQ_RESULT = "-999999999,null";
    protected static final Map<String, String> LATEST_ERRORS = new ConcurrentHashMap<>();
    private final FetchMySQLSequnceHandler mysqlSeqFetcher = new FetchMySQLSequnceHandler();
    private static final IncrSequenceMySQLHandler INSTANCE = new IncrSequenceMySQLHandler();

    public static IncrSequenceMySQLHandler getInstance() {
        return IncrSequenceMySQLHandler.INSTANCE;
    }

    public void load(boolean isLowerCaseTableNames) {
        // load sequence properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS, isLowerCaseTableNames);
        removeDesertedSequenceVals(props);
        putNewSequenceVals(props);
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
        }
    }

    /**
     * save sequence -> curval
     */
    private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<>();

    @Override
    public long nextId(String seqName) {
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

    private Long getNextValidSeqVal(SequenceVal seqVal) {
        Long nexVal = seqVal.nextValue();
        if (seqVal.isNexValValid(nexVal)) {
            return nexVal;
        } else {
            seqVal.fetching.compareAndSet(true, false);
            return getSeqValueFromDB(seqVal);
        }
    }

    private long getSeqValueFromDB(SequenceVal seqVal) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("get next segement of sequence from db for sequence:" +
                    seqVal.seqName + " curVal " + seqVal.curVal);
        }
        if (seqVal.fetching.compareAndSet(false, true)) {
            seqVal.dbretVal = null;
            seqVal.dbfinished = false;
            seqVal.newValueSetted.set(false);
            mysqlSeqFetcher.execute(seqVal);
        }
        Long[] values = seqVal.waitFinish();
        if (values == null) {

            throw new RuntimeException("can't fetch sequence in db,sequence :" +
                    seqVal.seqName + " detail:" + mysqlSeqFetcher.getLastestError(seqVal.seqName));
        } else {
            if (seqVal.newValueSetted.compareAndSet(false, true)) {
                seqVal.setCurValue(values[0]);
                seqVal.maxSegValue = values[1];
                return values[0];
            } else {
                return seqVal.nextValue();
            }

        }

    }
}




