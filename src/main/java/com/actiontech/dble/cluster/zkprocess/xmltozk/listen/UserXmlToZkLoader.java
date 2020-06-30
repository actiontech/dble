/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;


import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class UserXmlToZkLoader implements NotifyService {
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public UserXmlToZkLoader(ZookeeperProcessListen zookeeperListen,
                             XmlProcessBase xmlParseBase) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Users.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ClusterLogic.syncUserXmlToCluster(xmlParseBase, gson);
        return true;
    }
}
