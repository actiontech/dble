package io.mycat.route.sequence.handler;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.MycatConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.util.PropertiesUtil;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IncrSequenceMySQLHandler implements SequenceHandler {

    protected static final Logger LOGGER = LoggerFactory
            .getLogger(IncrSequenceMySQLHandler.class);

    private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
    protected static final String errSeqResult = "-999999999,null";
    protected static Map<String, String> latestErrors = new ConcurrentHashMap<String, String>();
    private final FetchMySQLSequnceHandler mysqlSeqFetcher = new FetchMySQLSequnceHandler();
    private static final IncrSequenceMySQLHandler instance = new IncrSequenceMySQLHandler();

    public static IncrSequenceMySQLHandler getInstance() {
        return IncrSequenceMySQLHandler.instance;
    }

    public void load(boolean isLowerCaseTableNames) {
        // load sequnce properties
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
            if (!seqValueMap.containsKey(seqName)) {
                seqValueMap.put(seqName, new SequenceVal(seqName, dataNode));
            } else {
                seqValueMap.get(seqName).dataNode = dataNode;
            }
        }
    }

    /**
     * save sequnce -> curval
     */
    private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<String, SequenceVal>();

    @Override
    public long nextId(String seqName) {
        SequenceVal seqVal = seqValueMap.get(seqName);
        if (seqVal == null) {
            throw new ConfigException("can't find definition for sequence :"
                    + seqName);
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
            LOGGER.debug("get next segement of sequence from db for sequnce:"
                    + seqVal.seqName + " curVal " + seqVal.curVal);
        }
        if (seqVal.fetching.compareAndSet(false, true)) {
            seqVal.dbretVal = null;
            seqVal.dbfinished = false;
            seqVal.newValueSetted.set(false);
            mysqlSeqFetcher.execute(seqVal);
        }
        Long[] values = seqVal.waitFinish();
        if (values == null) {

            throw new RuntimeException("can't fetch sequnce in db,sequnce :"
                    + seqVal.seqName + " detail:"
                    + mysqlSeqFetcher.getLastestError(seqVal.seqName));
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




