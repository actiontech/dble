/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.PathMeta;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.NotifyService;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.ZookeeperProcessListen;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShardingZkToXmlListener implements NotifyService {
    private static final Logger LOGGER = LogManager.getLogger(ShardingZkToXmlListener.class);

    public ShardingZkToXmlListener(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {
        final PathMeta<RawJson> path = ClusterMetaUtil.getConfShardingPath();
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        final RawJson configValue = clusterHelper.getPathValue(path).map(ClusterValue::getData).orElse(null);
        if (configValue == null) {
            LOGGER.warn("receive empty value");
        }
        ClusterLogic.forConfig().syncShardingJson(path.getPath(), configValue);
    }


}
