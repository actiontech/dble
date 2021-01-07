/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.xmltoKv;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.general.response.*;


/**
 * Created by szf on 2018/1/29.
 */
public final class XmltoCluster {


    private XmltoCluster() {

    }

    public static void main(String[] args) {
        try {
            ClusterController.loadClusterProperties();
            ClusterGeneralConfig.initConfig();
            AbstractConsulSender sender = ((AbstractConsulSender) (ClusterGeneralConfig.getInstance().getClusterSender()));
            sender.initConInfo();
            initFileToUcore(sender);
            System.out.println("XmlToClusterMain Finished");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void initFileToUcore(AbstractConsulSender sender) throws Exception {
        ClusterClearKeyListener ucoreListen = new ClusterClearKeyListener(sender);


        new XmlDbLoader(ucoreListen);

        new XmlShardingLoader(ucoreListen);

        new XmlUserLoader(ucoreListen);

        new SequencePropertiesLoader(ucoreListen);

        ucoreListen.initAllNode();
        new DbGroupHaResponse().notifyCluster();
    }

}
