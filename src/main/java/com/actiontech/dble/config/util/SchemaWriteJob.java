package com.actiontech.dble.config.util;

import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.config.loader.zkprocess.entity.Schemas;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.ReadHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.WriteHost;
import com.actiontech.dble.singleton.HaConfigManager;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2019/10/23.
 */
public class SchemaWriteJob implements Runnable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PhysicalDNPoolSingleWH.class);
    private final Set<PhysicalDNPoolSingleWH> changeSet;
    private final Schemas schemas;
    private volatile boolean finish = false;
    private final ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public SchemaWriteJob(Set<PhysicalDNPoolSingleWH> changeSet, Schemas schemas) {
        this.changeSet = changeSet;
        this.schemas = schemas;
    }

    @Override
    public void run() {
        List<DataHost> dhlist = schemas.getDataHost();
        for (DataHost dh : dhlist) {
            for (PhysicalDNPoolSingleWH physicalDNPoolSingleWH : changeSet) {
                if (dh.getName().equals(physicalDNPoolSingleWH.getHostName())) {
                    changeHostInfo(dh, physicalDNPoolSingleWH);
                }
            }
        }
        HaConfigManager.getInstance().write(schemas);
        HaConfigManager.getInstance().finishAndNext();
        lock.lock();
        try {
            finish = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }


    private void changeHostInfo(DataHost dh, PhysicalDNPoolSingleWH physicalDNPoolSingleWH) {
        WriteHost w = new WriteHost();
        PhysicalDatasource ws = physicalDNPoolSingleWH.getSource();
        w.setDisabled("" + ws.isDisabled());
        w.setHost(ws.getConfig().getHostName());
        w.setId(ws.getConfig().getId());
        w.setUrl(ws.getConfig().getUrl());
        w.setWeight("" + ws.getConfig().getWeight());
        w.setUser(ws.getConfig().getUser());
        WriteHost ow = dh.getWriteHost().get(0);
        if (ow.getHost().equals(ws.getConfig().getHostName())) {
            w.setPassword(ow.getPassword());
            w.setUsingDecrypt(ow.getUsingDecrypt());
        } else {
            for (ReadHost rh : ow.getReadHost()) {
                if (rh.getId().equals(w.getId())) {
                    w.setPassword(rh.getPassword());
                    w.setUsingDecrypt(rh.getUsingDecrypt());
                }
            }
        }

        List<ReadHost> newReadList = new ArrayList<ReadHost>();
        for (PhysicalDatasource rs : physicalDNPoolSingleWH.getReadSources().get(0)) {
            ReadHost r = new ReadHost();
            r.setDisabled("" + rs.isDisabled());
            r.setHost(rs.getConfig().getHostName());
            r.setId(rs.getConfig().getId());
            r.setUrl(rs.getConfig().getUrl());
            r.setWeight("" + rs.getConfig().getWeight());
            r.setUser(rs.getConfig().getUser());
            WriteHost ow1 = dh.getWriteHost().get(0);
            if (ow1.getId().equals(rs.getConfig().getId())) {
                r.setPassword(ow.getPassword());
                r.setUsingDecrypt(ow.getUsingDecrypt());
            } else {
                for (ReadHost rh : ow1.getReadHost()) {
                    if (rh.getHost().equals(r.getHost())) {
                        r.setPassword(rh.getPassword());
                        r.setUsingDecrypt(rh.getUsingDecrypt());
                    }
                }
            }
            newReadList.add(r);
        }
        w.setReadHost(newReadList);
        ArrayList<WriteHost> wl = new ArrayList<WriteHost>();
        wl.add(w);
        dh.setWriteHost(wl);
    }


    public void waitForWritingDone() {
        lock.lock();
        try {
            while (!finish) {
                condition.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("unexpected error:", e);
        } finally {
            lock.unlock();
        }
    }
}
