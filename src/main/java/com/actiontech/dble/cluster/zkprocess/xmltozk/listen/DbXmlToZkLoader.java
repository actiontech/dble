/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;


import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;

public class DbXmlToZkLoader implements NotifyService {
    private XmlProcessBase xmlParseBase;

    public DbXmlToZkLoader(ZookeeperProcessListen zookeeperListen,
                           XmlProcessBase xmlParseBase) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(DbGroups.class);
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ClusterLogic.syncDbXmlToCluster(xmlParseBase);
        return true;
    }
}
