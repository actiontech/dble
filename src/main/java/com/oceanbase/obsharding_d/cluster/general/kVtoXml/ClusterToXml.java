/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.kVtoXml;

import com.oceanbase.obsharding_d.cluster.general.AbstractConsulSender;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterOffLineListener;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterSingleKeyListener;
import com.oceanbase.obsharding_d.cluster.general.response.*;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.OnlineType;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/24.
 */
public final class ClusterToXml {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterToXml.class);
    private static ClusterClearKeyListener listener = null;

    private static ClusterOffLineListener offlineStatusListener = null;

    private static List<Thread> threads = new ArrayList<>(3);

    private ClusterToXml() {

    }

    public static void loadKVtoFile(AbstractConsulSender sender) {
        try {
            //create a new listener to the ucore config change
            listener = new ClusterClearKeyListener(sender);
            //add all loader into listener map list
            new XmlDbLoader().registerPrefixForUcore(listener);
            new XmlShardingLoader().registerPrefixForUcore(listener);
            new XmlUserLoader().registerPrefixForUcore(listener);
            new SequencePropertiesLoader().registerPrefixForUcore(listener);


            //add listener to watch the Prefix of the keys
            new ConfigStatusResponse().registerPrefixForUcore(listener);
            final PauseShardingNodeResponse pauseShardingNodeResponse = new PauseShardingNodeResponse();
            pauseShardingNodeResponse.registerPrefixForUcore(listener);


            final ClusterSingleKeyListener binlogPauseListener = new ClusterSingleKeyListener(ClusterPathUtil.getBinlogPausePath() + SEPARATOR, new BinlogPauseStatusResponse(), sender);

            final ClusterSingleKeyListener ddlListener = new ClusterSingleKeyListener(ClusterPathUtil.getDDLPath() + SEPARATOR, new DdlChildResponse(), sender);

            final ClusterSingleKeyListener viewListener = new ClusterSingleKeyListener(ClusterPathUtil.getViewChangePath() + SEPARATOR, new ViewChildResponse(), sender);

            offlineStatusListener = new ClusterOffLineListener(sender);

            listener.initForXml();
            Thread thread = new Thread(listener);
            thread.setName("UCORE_KEY_LISTENER");
            thread.start();

            Thread thread4 = new Thread(offlineStatusListener);
            thread4.setName("ONLINE_UCORE_LISTENER");
            thread4.start();

            Thread thread6 = new Thread(binlogPauseListener);
            thread6.setName("BINLOG_PAUSE_UCORE_LISTENER");
            thread6.start();

            Thread thread2 = new Thread(ddlListener);
            thread2.setName("DDL_UCORE_LISTENER");
            threads.add(thread2);

            Thread thread3 = new Thread(viewListener);
            thread3.setName("VIEW_UCORE_LISTENER");
            threads.add(thread3);

            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                ClusterSingleKeyListener dbGroupHaListener = new ClusterSingleKeyListener(ClusterPathUtil.getHaBasePath(), new DbGroupHaResponse(), sender);
                Thread thread5 = new Thread(dbGroupHaListener);
                thread5.setName("DB_GROUP_HA_LISTENER");
                threads.add(thread5);
            }

        } catch (Exception e) {
            LOGGER.warn("loadKVtoFile", e);
        }
    }

    public static void startMetaListener() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    public static ClusterClearKeyListener getListener() {
        return listener;
    }

    public static Map<String, OnlineType> getOnlineMap() {
        return offlineStatusListener.copyOnlineMap();
    }
}
