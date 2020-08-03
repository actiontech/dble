/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.xmltoKv;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.response.*;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.model.ClusterConfig;


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

        new XmlDbLoader(xmlProcess, ucoreListen);

        new XmlShardingLoader(xmlProcess, ucoreListen);

        new XmlUserLoader(xmlProcess, ucoreListen);

        new XmlEhcachesLoader(xmlProcess, ucoreListen);

        new CacheserviceResponse(ucoreListen);

        new PropertySequenceLoader(ucoreListen);

        xmlProcess.initJaxbClass();
        ucoreListen.initAllNode();
        if (ClusterConfig.getInstance().isNeedSyncHa()) {
            new DbGroupHaResponse().notifyCluster();
        }
    }

}
