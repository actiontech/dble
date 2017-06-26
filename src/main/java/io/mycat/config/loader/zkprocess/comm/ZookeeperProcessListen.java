package io.mycat.config.loader.zkprocess.comm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 进行zookeeper操作的监控器器父类信息
 * 
 * @author liujun
 * 
 * @date 2015年2月4日
 * @vsersion 0.0.1
 */
public class ZookeeperProcessListen {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperProcessListen.class);

    private Set<NotifyService> initCache = new HashSet<>();
    private Map<String, NotifyService> watchMap = new HashMap<>();

    public void addToInit(NotifyService service) {
        initCache.add(service);
    }

    public void clearInited( ) {
        initCache.clear();
    }
    public void addWatch(String key, NotifyService service) {
        watchMap.put(key,service);
    }

    /**
     * 返回路径集合
    */
    public Set<String> getWatchPath() {
        return watchMap.keySet();
    }

    /**
     * 进行缓存更新通知
     *
     */
    public boolean notify(String key) {
        boolean result = false;
        if (null != key && !"".equals(key)) {
            NotifyService cacheService = watchMap.get(key);
            if (null != cacheService) {
                try {
                    result = cacheService.notifyProcess();
                } catch (Exception e) {
                    LOGGER.error("ZookeeperProcessListen notify key :" + key + " error:Exception info:", e);
                }
            }
        }
        return result;
    }

    /**
     * 进行通知所有缓存进行更新操作
     */
    public void initAllNode() {
        Iterator<NotifyService> notifyIter = initCache.iterator();
        NotifyService item;
        while (notifyIter.hasNext()) {
            item = notifyIter.next();
            // 进行缓存更新通知操作
            try {
                item.notifyProcess();
            } catch (Exception e) {
                LOGGER.error("ZookeeperProcessListen initAllNode :" + item + ";error:Exception info:", e);
            }
        }
    }

}
