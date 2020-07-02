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
import com.actiontech.dble.cluster.zkprocess.entity.Shardings;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ShardingZkToXmlListener implements NotifyService {
    private final Gson gson;
    private XmlProcessBase xmlParseBase;

    public ShardingZkToXmlListener(ZookeeperProcessListen zookeeperListen,
                                   XmlProcessBase xmlParseBase) {
        this.xmlParseBase = xmlParseBase;
        xmlParseBase.addParseClass(Shardings.class);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Table.class, new TableGsonAdapter());
        gson = gsonBuilder.create();
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        KvBean configValue = ClusterHelper.getKV(ClusterPathUtil.getConfShardingPath());
        if (configValue == null) {
            throw new RuntimeException(ClusterPathUtil.getConfShardingPath() + " is null");
        }
        ClusterLogic.syncShardingXmlToLocal(configValue, xmlParseBase, gson);
        return true;
    }


}
