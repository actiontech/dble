package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.ReadHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.WriteHost;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DataSourceStatus;
import com.actiontech.dble.config.util.SchemaWriteJob;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_LIST;
import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_NAME;
import static com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH.JSON_WRITE_SOURCE;

/**
 * Created by szf on 2019/10/23.
 */
public final class HaConfigManager {

    private static final HaConfigManager INSTANCE = new HaConfigManager();
    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;
    private static final String WRITEPATH = "schema.xml";
    private static Schemas schema;
    private final AtomicBoolean isWriting = new AtomicBoolean(false);
    private final ReentrantReadWriteLock adjustLock = new ReentrantReadWriteLock();
    private volatile SchemaWriteJob schemaWriteJob;
    private volatile Set<PhysicalDNPoolSingleWH> waitingSet = new HashSet<>();

    private HaConfigManager() {
        try {
            XmlProcessBase xmlProcess = new XmlProcessBase();
            xmlProcess.addParseClass(Schemas.class);
            xmlProcess.initJaxbClass();
            this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlProcess);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        schema = this.parseSchemaXmlService.parseXmlToBean(WRITEPATH);
        return;
    }

    public void write(Schemas schemas) {
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;
        this.parseSchemaXmlService.parseToXmlWrite(schemas, path, "schema");
    }

    public void finishAndNext() {
        isWriting.set(false);
        triggerNext();
    }

    public void updateConfDataHost(PhysicalDNPoolSingleWH physicalDNPoolSingleWH, boolean syncWriteConf) {
        SchemaWriteJob thisTimeJob = null;
        //check if there is one thread is writing
        if (isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                waitingSet.add(physicalDNPoolSingleWH);
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                thisTimeJob = schemaWriteJob;
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(schemaWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        } else {
            adjustLock.readLock().lock();
            try {
                thisTimeJob = schemaWriteJob;
                waitingSet.add(physicalDNPoolSingleWH);
            } finally {
                adjustLock.readLock().unlock();
            }
            triggerNext();
        }
        if (syncWriteConf) {
            //waitDone
            thisTimeJob.waitForWritingDone();
        }
    }


    public void triggerNext() {
        if (waitingSet.size() != 0 && isWriting.compareAndSet(false, true)) {
            adjustLock.writeLock().lock();
            try {
                schemaWriteJob = new SchemaWriteJob(waitingSet, schema);
                waitingSet = new HashSet<>();
                DbleServer.getInstance().getComplexQueryExecutor().execute(schemaWriteJob);
            } finally {
                adjustLock.writeLock().unlock();
            }
        }
    }

    public Map<String, String> getSourceJsonList() {
        Map<String, String> map = new HashMap<>();
        for (DataHost dh : schema.getDataHost()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JSON_NAME, dh.getName());
            List<DataSourceStatus> list = new ArrayList<>();
            for (WriteHost wh : dh.getWriteHost()) {
                list.add(new DataSourceStatus(wh.getHost(), "true".equals(wh.getDisabled()), false));
                jsonObject.put(JSON_WRITE_SOURCE, new DataSourceStatus(wh.getHost(), "true".equals(wh.getDisabled()), false));
                for (ReadHost rh : wh.getReadHost()) {
                    list.add(new DataSourceStatus(rh.getHost(), "true".equals(rh.getDisabled()), true));
                }
            }
            jsonObject.put(JSON_LIST, list);
            map.put(dh.getName(), jsonObject.toJSONString());
        }
        return map;
    }

    public static HaConfigManager getInstance() {
        return INSTANCE;
    }
}
