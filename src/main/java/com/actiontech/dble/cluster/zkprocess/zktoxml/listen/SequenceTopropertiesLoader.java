/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkData;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * SequenceTopropertiesLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SequenceTopropertiesLoader extends ZkMultiLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTopropertiesLoader.class);

    private final String currZkPath;

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    private ZookeeperProcessListen zookeeperListen;

    public SequenceTopropertiesLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        this.zookeeperListen = zookeeperListen;
        currZkPath = ClusterPathUtil.getSequencesCommonPath();
        this.zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ZkData data = this.getTreeDirectory(currZkPath, ClusterPathUtil.SEQUENCE_COMMON, true);


        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_CONF, data.getChildren());

        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_DB_CONF, data.getChildren());
        return true;
    }

    private void sequenceZkToProperties(String name, Map<String, ZkData> dataMap) throws Exception {
        if (dataMap.size() > 0) {
            String writeFile = name + PROPERTIES_SUFFIX;
            ZkData data = dataMap.get(writeFile);
            if (data != null) {
                ConfFileRWUtils.writeFile(data.getName(), data.getValue());
            }
            String sequenceWatchPath = ClusterPathUtil.getSequencesCommonPath() + ClusterPathUtil.SEPARATOR + writeFile;
            this.zookeeperListen.addWatch(sequenceWatchPath, this);
            LOGGER.info("SequenceTozkLoader notifyProcess " + name + " to local properties success");
        }
    }

}
