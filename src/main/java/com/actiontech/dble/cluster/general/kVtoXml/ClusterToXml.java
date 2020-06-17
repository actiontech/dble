/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.kVtoXml;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.general.listener.ClusterOffLineListener;
import com.actiontech.dble.cluster.general.listener.ClusterSingleKeyListener;
import com.actiontech.dble.cluster.general.response.*;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.ClusterConfig;

import java.util.Map;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/24.
 */
public final class ClusterToXml {

    private static ClusterClearKeyListener listener = null;

    private static ClusterSingleKeyListener ddlListener = null;

    private static ClusterSingleKeyListener viewListener = null;

    private static ClusterSingleKeyListener dbGroupHaListener = null;

    private static ClusterOffLineListener onlineListener = null;


    private ClusterToXml() {

    }

    public static void loadKVtoFile() {
        try {
            //create a new listener to the ucore config change
            listener = new ClusterClearKeyListener();
            XmlProcessBase xmlProcess = new XmlProcessBase();
            //add all loader into listener map list
            new XmlDbLoader(xmlProcess, listener);
            new XmlShardingLoader(xmlProcess, listener);
            new XmlUserLoader(xmlProcess, listener);
            new PropertySequenceLoader(listener);
            xmlProcess.initJaxbClass();

            //add listener to watch the Prefix of the keys
            new ConfigStatusResponse(listener);
            new BinlogPauseStatusResponse(listener);
            new PauseShardingNodeResponse(listener);

            dbGroupHaListener = new ClusterSingleKeyListener(ClusterPathUtil.getHaBasePath(), new DbGroupHaResponse());

            ddlListener = new ClusterSingleKeyListener(ClusterPathUtil.getDDLPath() + SEPARATOR, new DdlChildResponse());

            viewListener = new ClusterSingleKeyListener(ClusterPathUtil.getViewChangePath() + SEPARATOR, new ViewChildResponse());

            onlineListener = new ClusterOffLineListener();

            listener.initForXml();
            Thread thread = new Thread(listener);
            thread.setName("UCORE_KEY_LISTENER");
            thread.start();

            Thread thread2 = new Thread(ddlListener);
            thread2.setName("DDL_UCORE_LISTENER");
            thread2.start();

            Thread thread3 = new Thread(viewListener);
            thread3.setName("VIEW_UCORE_LISTENER");
            thread3.start();

            Thread thread4 = new Thread(onlineListener);
            thread4.setName("ONLINE_UCORE_LISTENER");
            thread4.start();

            if (ClusterConfig.getInstance().isNeedSyncHa()) {
                Thread thread5 = new Thread(dbGroupHaListener);
                thread5.setName("DB_GROUP_HA_LISTENER");
                thread5.start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ClusterClearKeyListener getListener() {
        return listener;
    }

    public static Map<String, String> getOnlineMap() {
        return onlineListener.copyOnlineMap();
    }
}
