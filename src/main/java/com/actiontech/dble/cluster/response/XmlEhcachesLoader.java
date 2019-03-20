/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.entity.cache.Ehcache;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by szf on 2018/1/29.
 */
public class XmlEhcachesLoader implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlEhcachesLoader.class);

    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    private static final String WRITEPATH = "ehcache.xml";

    private static final String CONFIG_PATH = ClusterPathUtil.getEhcacheNamePath();

    public XmlEhcachesLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        if (jsonObj.get(ClusterPathUtil.EHCACHE) != null) {
            Ehcache ehcache = parseJsonEhcacheService.parseJsonToBean(jsonObj.getJSONObject(ClusterPathUtil.EHCACHE).toJSONString());
            String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH);
            path = new File(path).getPath() + File.separator + WRITEPATH;
            this.parseEcacheXMl.parseToXmlWrite(ehcache, path, null);
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        Ehcache ehcache = this.parseEcacheXMl.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject ehcacheObj = new JSONObject();
        ehcacheObj.put(ClusterPathUtil.EHCACHE, ehcache);
        ClusterHelper.setKV(CONFIG_PATH, ehcacheObj.toJSONString());
    }
}
