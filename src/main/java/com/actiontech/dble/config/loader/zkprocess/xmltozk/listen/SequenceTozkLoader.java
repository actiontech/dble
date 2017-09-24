/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;

import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.Properties;

/**
 * SequenceTozkLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class SequenceTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTozkLoader.class);

    /**
     * currZkPath
     */
    private final String currZkPath;

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";


    public SequenceTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getSequencesPath();
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {

        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_conf to zk success");

        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DB_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_db_conf to zk success");

        this.sequenceTozk(currZkPath, PROPERTIES_SEQUENCE_DISTRIBUTED_CONF);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_distributed_conf to zk success");


        return true;
    }

    /**
     * sequenceTozk
     *
     * @param basePath
     * @param name
     * @throws Exception
     * @Created 2016/9/17
     */
    private void sequenceTozk(String basePath, String name) throws Exception {
        String readFile = name + PROPERTIES_SUFFIX;
        String commSequence = ConfFileRWUtils.readFile(readFile);
        if (name.equals(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF)) {
            Properties props = new Properties();
            props.load(new StringReader(commSequence));
            if (!"ZK".equals(props.getProperty("INSTANCEID"))) {
                LOGGER.info("The property of INSTANCEID in " + readFile + " is not zk,no need to store in zk");
                return;
            }
        }
        String sequenceZkPath = ZKPaths.makePath(KVPathUtil.SEQUENCE_COMMON, readFile);
        this.checkAndwriteString(basePath, sequenceZkPath, commSequence);
    }
}
