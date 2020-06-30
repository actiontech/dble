/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlShardingLoader implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlShardingLoader.class);
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public XmlShardingLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Shardings.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        gson = gsonBuilder.create();
        confListener.addChild(this, ClusterPathUtil.getConfShardingPath());
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        ClusterLogic.syncShardingXmlToLocal(configValue, xmlParseBase, gson);
    }

    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.syncShardingXmlToCluster(xmlParseBase, gson);
    }


}
