/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class XmlUserLoader implements ClusterXmlLoader {
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public XmlUserLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Users.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        gson = gsonBuilder.create();
        confListener.addChild(this, ClusterPathUtil.getUserConfPath());
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        ClusterLogic.syncUserXmlToLocal(configValue, xmlParseBase, gson);
    }

    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.syncUserXmlToCluster(xmlParseBase, gson);
    }
}
