package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.alarm.UcoreInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/2/2.
 */
public class UcoreSingleKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreSingleKeyListener.class);
    private long index = 0;
    UcoreXmlLoader child;
    String path;

    private Map<String, String> cache = new HashMap<>();


    @Override
    public void run() {
        for (; ; ) {
            try {
                UcoreInterface.SubscribeKvPrefixInput input
                        = UcoreInterface.SubscribeKvPrefixInput.newBuilder().setIndex(index).setDuration(60).setKeyPrefix(path).build();
                UcoreInterface.SubscribeKvPrefixOutput output = ClusterUcoreSender.subscribeKvPrefix(input);
                Map<String, UKvBean> diffMap = getDiffMap(output);
                if (output.getIndex() != index) {
                    handle(diffMap);
                    index = output.getIndex();
                }
            } catch (Exception e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            }
        }
    }


    public UcoreSingleKeyListener(String path, UcoreXmlLoader child) {
        this.child = child;
        this.path = path;
    }

    public void handle(Map<String, UKvBean> diffMap) {
        try {
            for (Map.Entry<String, UKvBean> entry : diffMap.entrySet()) {
                child.notifyProcess(entry.getValue());
            }
        } catch (Exception e) {
            LOGGER.warn(" ucore event handle error");
            e.printStackTrace();
        }
    }


    private Map<String, UKvBean> getDiffMap(UcoreInterface.SubscribeKvPrefixOutput output) {
        Map<String, UKvBean> diffMap = new HashMap<String, UKvBean>();
        Map<String, String> newKeyMap = new HashMap<String, String>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            newKeyMap.put(output.getKeys(i), output.getValues(i));
            if (cache.get(output.getKeys(i)) != null) {
                if (!cache.get(output.getKeys(i)).equals(output.getValues(i))) {
                    diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.UPDATE));
                }
            } else {
                diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD));
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                diffMap.put(entry.getKey(), new UKvBean(entry.getKey(), entry.getValue(), UKvBean.DELETE));
            }
        }

        cache = newKeyMap;

        return diffMap;
    }
}
