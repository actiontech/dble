/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DiretoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SequenceTopropertiesLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SequenceTopropertiesLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTopropertiesLoader.class);

    private final String currZkPath;

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";

    private ZookeeperProcessListen zookeeperListen;

    public SequenceTopropertiesLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        this.zookeeperListen = zookeeperListen;
        currZkPath = KVPathUtil.getSequencesPath();
        this.zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DiretoryInf sequenceDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.SEQUENCES, sequenceDirectory);

        sequenceDirectory = (DiretoryInf) sequenceDirectory.getSubordinateInfo().get(0);

        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_CONF, sequenceDirectory);

        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_DB_CONF, sequenceDirectory);

        this.sequenceZkToProperties(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF, sequenceDirectory);
        return true;
    }

    /**
     * sequenceZkToProperties
     *
     * @param name schema
     * @throws Exception
     */
    private void sequenceZkToProperties(String name, DiretoryInf seqDirectory) throws Exception {
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory, KVPathUtil.SEQUENCE_COMMON);

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            ZkDataImpl commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            if (commData != null) {
                ConfFileRWUtils.writeFile(commData.getName(), commData.getValue());
            }
            String sequenceWatchPath = KVPathUtil.getSequencesCommonPath() + writeFile;
            this.zookeeperListen.addWatch(sequenceWatchPath, this);
            LOGGER.info("SequenceTozkLoader notifyProcess " + name + " to local properties success");
        }
    }

}
