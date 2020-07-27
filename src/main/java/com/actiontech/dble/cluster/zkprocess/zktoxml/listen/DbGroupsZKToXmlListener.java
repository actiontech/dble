/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;

public class DbGroupsZKToXmlListener implements NotifyService {
    private XmlProcessBase xmlParseBase;

    public DbGroupsZKToXmlListener(ZookeeperProcessListen zookeeperListen,
                                   XmlProcessBase xmlParseBase) {
        zookeeperListen.addToInit(this);
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(DbGroups.class);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        KvBean configValue = ClusterHelper.getKV(ClusterPathUtil.getDbConfPath());
        if (configValue == null) {
            throw new RuntimeException(ClusterPathUtil.getDbConfPath() + " is null");
        }
        ClusterLogic.syncDbXmlToLocal(xmlParseBase, configValue);
        return true;
    }


}
