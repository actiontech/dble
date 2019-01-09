/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.alarm.UcoreInterface;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/1/24.
 */
public class UcoreClearKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreClearKeyListener.class);

    private Map<String, UcoreXmlLoader> childService = new HashMap<>();

    private Map<String, String> cache = new HashMap<>();

    private long index = 0;


    @Override
    public void run() {
        for (; ; ) {
            try {
                UcoreInterface.SubscribeKvPrefixInput input
                        = UcoreInterface.SubscribeKvPrefixInput.newBuilder().setIndex(index).setDuration(60).setKeyPrefix(UcorePathUtil.CONF_BASE_PATH).build();
                UcoreInterface.SubscribeKvPrefixOutput output = ClusterUcoreSender.subscribeKvPrefix(input);
                if (output.getIndex() != index) {
                    Map<String, UKvBean> diffMap = getDiffMapWithOrder(output);
                    handle(diffMap);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }


    private Map<String, UKvBean> getDiffMapWithOrder(UcoreInterface.SubscribeKvPrefixOutput output) {
        Map<String, UKvBean> diffMap = new LinkedHashMap<String, UKvBean>();
        Map<String, String> newKeyMap = new LinkedHashMap<String, String>();

        UKvBean reloadKv = null;

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            newKeyMap.put(output.getKeys(i), output.getValues(i));
            if (cache.get(output.getKeys(i)) != null) {
                if (!cache.get(output.getKeys(i)).equals(output.getValues(i))) {
                    if (output.getKeys(i).equalsIgnoreCase(UcorePathUtil.getConfStatusPath())) {
                        reloadKv = new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.UPDATE);
                    } else {
                        diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.UPDATE));
                    }
                }
            } else {
                if (output.getKeys(i).equalsIgnoreCase(UcorePathUtil.getConfStatusPath())) {
                    reloadKv = new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD);
                } else {
                    diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD));
                }
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                if (entry.getKey().equalsIgnoreCase(UcorePathUtil.getConfStatusPath())) {
                    reloadKv = new UKvBean(entry.getKey(), entry.getValue(), UKvBean.DELETE);
                } else {
                    diffMap.put(entry.getKey(), new UKvBean(entry.getKey(), entry.getValue(), UKvBean.DELETE));
                }
            }
        }

        if (reloadKv != null) {
            diffMap.put(reloadKv.getKey(), reloadKv);
        }

        cache = newKeyMap;

        return diffMap;
    }

    public void initForXml() {
        try {
            UcoreInterface.SubscribeKvPrefixInput input
                    = UcoreInterface.SubscribeKvPrefixInput.newBuilder().setIndex(0).setDuration(60).setKeyPrefix(UcorePathUtil.BASE_PATH).build();
            UcoreInterface.SubscribeKvPrefixOutput output = ClusterUcoreSender.subscribeKvPrefix(input);
            index = output.getIndex();
            Map<String, UKvBean> diffMap = new HashMap<String, UKvBean>();
            for (int i = 0; i < output.getKeysCount(); i++) {
                diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD));
                cache.put(output.getKeys(i), output.getValues(i));
            }
            handle(diffMap);
        } catch (Exception e) {
            LOGGER.warn("error when start up dble,ucore connect error");
        }
    }


    /**
     * handle the back data from the subscribe
     * if the config version changes,write the file
     * or just start a new waiting
     */
    public void handle(Map<String, UKvBean> diffMap) {
        try {
            for (Map.Entry<String, UKvBean> entry : diffMap.entrySet()) {
                UcoreXmlLoader x = childService.get(entry.getKey());
                if (x != null) {
                    x.notifyProcess(entry.getValue());
                }
            }
        } catch (Exception e) {
            LOGGER.warn(" ucore data parse to xml error ", e);
        }
    }


    /**
     * add ucoreXmlLoader into the watch list
     * every loader knows the path of it self
     *
     * @param loader
     * @param path
     */
    public void addChild(UcoreXmlLoader loader, String path) {
        this.childService.put(path, loader);
    }

    public UcoreXmlLoader getReponse(String key) {
        return this.childService.get(key);
    }

    public void initAllNode() {
        for (Map.Entry<String, UcoreXmlLoader> service : childService.entrySet()) {
            try {
                service.getValue().notifyCluster();
            } catch (Exception e) {
                LOGGER.warn(" UcoreClearKeyListener init all node error:", e);
            }
        }
    }

    public long getIndex() {
        return index;
    }
}
