/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.xmltoKv;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.response.*;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.singleton.HaConfigManager;

import java.util.Map;


/**
 * Created by szf on 2018/1/29.
 */
public final class XmltoCluster {


    private XmltoCluster() {

    }

    public static void main(String[] args) throws Exception {
        ClusterController.initFromShellUcore();
        initFileToUcore();
        System.out.println("XmltoZkMain Finished");
    }

    public static void initFileToUcore() throws Exception {
        ClusterClearKeyListener ucoreListen = new ClusterClearKeyListener();

        XmlProcessBase xmlProcess = new XmlProcessBase();

        new XmlRuleLoader(xmlProcess, ucoreListen);

        new XmlServerLoader(xmlProcess, ucoreListen);

        new XmlSchemaLoader(xmlProcess, ucoreListen);

        new XmlEhcachesLoader(xmlProcess, ucoreListen);

        new CacheserviceResponse(ucoreListen);

        new PropertySequenceLoader(ucoreListen);

        xmlProcess.initJaxbClass();
        ucoreListen.initAllNode();
        if (ClusterHelper.useClusterHa()) {
            new DataHostHaResponse().notifyCluster();
        } else {
            Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(entry.getKey()), entry.getValue());
            }
        }
    }

}
