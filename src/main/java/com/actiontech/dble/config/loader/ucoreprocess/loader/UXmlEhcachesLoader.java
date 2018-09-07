package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
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
public class UXmlEhcachesLoader implements UcoreXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(UXmlEhcachesLoader.class);

    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    private static final String WRITEPATH = "ehcache.xml";

    private static final String CONFIG_PATH = UcorePathUtil.getEhcacheNamePath();

    public UXmlEhcachesLoader(XmlProcessBase xmlParseBase, UcoreClearKeyListener confListener) {
        this.parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
        if (jsonObj.get(UcorePathUtil.EHCACHE) != null) {
            Ehcache ehcache = parseJsonEhcacheService.parseJsonToBean(jsonObj.getJSONObject(UcorePathUtil.EHCACHE).toJSONString());
            String path = ResourceUtil.getResourcePathFromRoot(UcorePathUtil.UCORE_LOCAL_WRITE_PATH);
            path = new File(path).getPath() + File.separator + WRITEPATH;
            this.parseEcacheXMl.parseToXmlWrite(ehcache, path, null);
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        Ehcache ehcache = this.parseEcacheXMl.parseXmlToBean(UcorePathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject ehcacheObj = new JSONObject();
        ehcacheObj.put(UcorePathUtil.EHCACHE, ehcache);
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, ehcacheObj.toJSONString());
    }
}
