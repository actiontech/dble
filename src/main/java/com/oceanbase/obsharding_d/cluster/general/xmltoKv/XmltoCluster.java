/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.xmltoKv;

import com.oceanbase.obsharding_d.cluster.ClusterController;
import com.oceanbase.obsharding_d.cluster.ClusterGeneralConfig;
import com.oceanbase.obsharding_d.cluster.general.AbstractConsulSender;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.general.response.*;
import com.oceanbase.obsharding_d.config.loader.SystemConfigLoader;


/**
 * Created by szf on 2018/1/29.
 */
public final class XmltoCluster {


    private XmltoCluster() {

    }

    public static void main(String[] args) {
        try {
            SystemConfigLoader.initSystemConfig();
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

        new XmlDbLoader().registerPrefixForUcore(ucoreListen);
        new XmlShardingLoader().registerPrefixForUcore(ucoreListen);
        new XmlUserLoader().registerPrefixForUcore(ucoreListen);
        new SequencePropertiesLoader().registerPrefixForUcore(ucoreListen);


        ucoreListen.initAllNode();
        new DbGroupHaResponse().notifyCluster();
    }

}
