/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zookeeper.process;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZkMultiLoader {


    private static final Logger LOGGER = LoggerFactory.getLogger(ZkMultiLoader.class);

    private CuratorFramework curator;


    public ZkData getTreeDirectory(String path, String name, boolean needChild) throws Exception {
        boolean check = this.checkPathExists(path);
        if (check) {
            String currDate = this.getDataToString(path);
            ZkData zkData = new ZkData(name, currDate);
            if (needChild) {
                List<String> childPathList = this.getChildNames(path);
                if (null != childPathList && !childPathList.isEmpty()) {
                    for (String childPath : childPathList) {
                        ZkData zkChildData = this.getTreeDirectory(ZKPaths.makePath(path, childPath), childPath, true);
                        zkData.addChild(zkChildData);
                    }
                }
            }
            return zkData;
        }
        return null;
    }

    protected boolean checkPathExists(String path) {
        try {
            Stat state = this.curator.checkExists().forPath(path);

            if (null != state) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.warn("checkPathExists " + path + "Exception ", e);
        }
        return false;
    }

    /**
     * get data from zookeeper and convert to string with check not null.
     */
    protected String getDataToString(String path) throws Exception {
        byte[] raw = curator.getData().forPath(path);

        checkNotNull(raw, "data of " + path + " must be not null!");
        return byteToString(raw);
    }

    /**
     * get child node name list based on path from zookeeper.
     *
     * @throws Exception
     */
    protected List<String> getChildNames(String path) throws Exception {
        return curator.getChildren().forPath(path);
    }

    protected void checkAndWriteString(String parentPath, String currPath, String value) throws Exception {
        checkNotNull(parentPath, "data of path" + parentPath + " must be not null!");
        checkNotNull(currPath, "data of path" + currPath + " must be not null!");
        checkNotNull(value, "data of value:" + value + " must be not null!");

        String nodePath = ZKPaths.makePath(parentPath, currPath);

        Stat stat = curator.checkExists().forPath(nodePath);

        if (null == stat) {
            this.createPath(nodePath);
        }

        LOGGER.debug("ZkMultiLoader write file :" + nodePath + ", value :" + value);

        curator.setData().inBackground().forPath(nodePath, value.getBytes());

    }

    public void checkAndWriteString(String path, String value) throws Exception {
        checkNotNull(path, "data of path" + path + " must be not null!");
        checkNotNull(value, "data of value:" + value + " must be not null!");

        Stat stat = curator.checkExists().forPath(path);

        if (null == stat) {
            this.createPath(path);
        }
        curator.setData().forPath(path, value.getBytes());
    }


    public boolean createPath(String path) {

        LOGGER.trace("createPath child path is {}", path);

        boolean result = true;
        try {
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
        } catch (Exception e) {
            LOGGER.warn(" createPath error", e);
            result = false;
        }

        return result;
    }

    /**
     * raw byte data to string
     */
    protected String byteToString(byte[] raw) {
        // return empty json {}.
        if (raw.length == 0) {
            return "{}";
        }
        return new String(raw, StandardCharsets.UTF_8);
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }

}
