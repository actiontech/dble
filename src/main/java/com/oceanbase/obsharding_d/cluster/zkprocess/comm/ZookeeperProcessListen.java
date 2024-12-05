/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.comm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * ZookeeperProcessListen
 *
 * @author liujun
 * @date 2015/2/4
 * @vsersion 0.0.1
 */
public class ZookeeperProcessListen {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperProcessListen.class);

    private final Set<NotifyService> initCache = new HashSet<>();

    public void addToInit(NotifyService service) {
        initCache.add(service);
    }

    public void clearInited() {
        initCache.clear();
    }


    public void initAllNode() throws Exception {
        Iterator<NotifyService> notifyIter = initCache.iterator();
        NotifyService item;
        while (notifyIter.hasNext()) {
            item = notifyIter.next();
            try {
                item.notifyProcess();
            } catch (Exception e) {
                LOGGER.warn("ZookeeperProcessListen initAllNode :" + item + ";error:Exception info:", e);
                throw e;
            }
        }
    }

}
