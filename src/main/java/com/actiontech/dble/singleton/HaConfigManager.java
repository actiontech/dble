package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.HaChangeStatus;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.values.DbInstanceStatus;
import com.actiontech.dble.cluster.values.HaInfo;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBInstance;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.util.DbXmlWriteJob;
import com.actiontech.dble.util.ResourceUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.JSON_LIST;
import static com.actiontech.dble.backend.datasource.PhysicalDbGroup.JSON_NAME;

/**
 * Created by szf on 2019/10/23.
 */
public final class HaConfigManager {
    private static final String HA_LOG = "ha_log";
    private static final Logger HA_LOGGER = LoggerFactory.getLogger(HA_LOG);
    private static final HaConfigManager INSTANCE = new HaConfigManager();
    private XmlProcessBase xmlProcess = new XmlProcessBase();
    private DbGroups dbGroups;
    private AtomicInteger indexCreater = new AtomicInteger();
    private final AtomicBoolean isWriting = new AtomicBoolean(false);
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();
    private volatile DbXmlWriteJob dbXmlWriteJob;
    private volatile Set<PhysicalDbGroup> waitingSet = new HashSet<>();
    private final Map<Integer, HaChangeStatus> unfinised = new ConcurrentHashMap<>();
    private final AtomicInteger reloadIndex = new AtomicInteger();

    private HaConfigManager() {
        try {
            xmlProcess.addParseClass(DbGroups.class);
            xmlProcess.initJaxbClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void init() throws Exception {
        try {
            INSTANCE.dbGroups = (DbGroups) xmlProcess.baseParseXmlToBean(ConfigFileName.DB_XML);
        } catch (Exception e) {
            HA_LOGGER.warn("DbParseXmlImpl parseXmlToBean JAXBException", e);
            throw e;
        }
        reloadIndex.incrementAndGet();
        //try to clear the waiting list and
        if (dbXmlWriteJob != null) {
            dbXmlWriteJob.signalAll();
        }
        waitingSet = new HashSet<>();
    }

    public void write(DbGroups dbs, int reloadId) throws IOException {
        HA_LOGGER.info("try to writeDirectly DbGroups into local file " + reloadId);
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            if (reloadIndex.get() == reloadId) {
                String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
                path = new File(path).getPath() + File.separator + ConfigFileName.DB_XML;
                this.xmlProcess.safeParseWriteToXml(dbs, path, "db");
            } else {
                HA_LOGGER.info("reloadId changes when try to writeDirectly the local file,just skip " + reloadIndex.get());
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void finishAndNext() {
        isWriting.set(false);
        triggerNext();
    }

    public void updateDbGroupConf(PhysicalDbGroup dbGroup, boolean syncWriteConf) {
        DbXmlWriteJob thisTimeJob;
        HA_LOGGER.info("start to update the local file with sync flag " + syncWriteConf);
        //check if there is one thread is writing
        if (isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                HA_LOGGER.info("get into writeDirectly process");
                waitingSet.add(dbGroup);
                dbXmlWriteJob = new DbXmlWriteJob(waitingSet, dbGroups, reloadIndex.get());
                thisTimeJob = dbXmlWriteJob;
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(dbXmlWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        } else {
            adjustLock.readLock().lock();
            try {
                HA_LOGGER.info("get into merge process");
                thisTimeJob = dbXmlWriteJob;
                waitingSet.add(dbGroup);
            } finally {
                adjustLock.readLock().unlock();
            }
            triggerNext();
        }
        if (syncWriteConf && thisTimeJob != null) {
            //waitDone
            thisTimeJob.waitForWritingDone();
        }
    }


    private void triggerNext() {
        if (waitingSet.size() != 0 && isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                dbXmlWriteJob = new DbXmlWriteJob(waitingSet, dbGroups, reloadIndex.get());
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(dbXmlWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        }
    }

    public Map<String, String> getSourceJsonList() {
        Map<String, String> map = new HashMap<>();
        for (DBGroup dbGroup : dbGroups.getDbGroup()) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty(JSON_NAME, dbGroup.getName());
            List<DbInstanceStatus> list = new ArrayList<>();
            for (DBInstance dbInstance : dbGroup.getDbInstance()) {
                list.add(new DbInstanceStatus(dbInstance.getName(), "true".equals(dbInstance.getDisabled()), dbInstance.getPrimary() != null && dbInstance.getPrimary()));
            }
            Gson gson = new Gson();
            jsonObject.add(JSON_LIST, gson.toJsonTree(list));
            map.put(dbGroup.getName(), gson.toJson(jsonObject));
        }
        return map;
    }

    public static HaConfigManager getInstance() {
        return INSTANCE;
    }

    public int haStart(HaInfo.HaStage stage, HaInfo.HaStartType type, String command) {
        int id = indexCreater.getAndAdd(1);
        HaChangeStatus newStatus = new HaChangeStatus(id, command, type, System.currentTimeMillis(), stage);
        INSTANCE.unfinised.put(id, newStatus);
        HA_LOGGER.info("[HA] id = " + id +
                " start of Ha event type =" + type.toString() +
                " command = " + command +
                " stage = " + stage.toString());
        return id;
    }

    public void haFinish(int id, String errorMsg, String result) {
        HaChangeStatus status = unfinised.get(id);
        unfinised.remove(id);
        StringBuilder resultString = new StringBuilder().
                append("[HA] id = ").append(id).
                append(" end of Ha event type =").append(status.getType().toString()).
                append(" command = ").append(status.getCommand()).
                append(" stage = ").append(status.getStage().toString()).
                append(" finish type = \"").append(errorMsg == null ? "success" : errorMsg).append("\"");
        if (result != null) {
            resultString.append("\n result status of dbGroup :").append(result);
        }
        if (errorMsg == null) {
            HA_LOGGER.info(resultString.toString());
        } else {
            HA_LOGGER.warn(resultString.toString());
        }
    }

    public void haWaitingOthers(int id) {
        HaChangeStatus status = unfinised.get(id);
        status.setStage(HaInfo.HaStage.WAITING_FOR_OTHERS);
        HA_LOGGER.info("[HA] id = " + id +
                " ha waiting for others type =" + status.getType().toString() +
                " command = " + status.getCommand() +
                " stage = " + status.getStage().toString());
    }

    public void log(String log, Exception e) {
        HA_LOGGER.info("[HA] " + log, e);
    }

    public Map<Integer, HaChangeStatus> getUnfinised() {
        return unfinised;
    }


}
