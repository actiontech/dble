/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DirectoryInf;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ZkMultiLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class ZkMultiLoader {


    private static final Logger LOGGER = LoggerFactory.getLogger(ZkMultiLoader.class);

    private CuratorFramework curator;

    private Gson gson = new Gson();

    /**
     * getTreeDirectory
     *
     * @param path
     * @param zkDirectory
     * @throws Exception
     * @Created 2016/9/15
     */
    public void getTreeDirectory(String path, String name, DirectoryInf zkDirectory) throws Exception {

        boolean check = this.checkPathExists(path);

        if (check) {
            String currDate = this.getDataToString(path);

            List<String> childPathList = this.getChildNames(path);

            if (null != childPathList && !childPathList.isEmpty()) {
                DirectoryInf directory = new ZkDirectoryImpl(name, currDate);

                zkDirectory.add(directory);

                for (String childPath : childPathList) {
                    this.getTreeDirectory(ZKPaths.makePath(path, childPath), childPath, directory);
                }
            } else {
                zkDirectory.add(new ZkDataImpl(name, currDate));
            }
        }
    }

    /**
     * @param path
     * @return
     * @Created 2016/9/21
     */
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

    protected void writeZkString(String path, String value) throws Exception {
        checkNotNull(path, "data of path" + path + " must be not null!");
        checkNotNull(value, "data of value:" + value + " must be not null!");

        curator.setData().forPath(path, value.getBytes());
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

    /**
     * getZkData
     *
     * @param zkDirectory
     * @param name
     * @return
     * @Created 2016/9/16
     */
    protected DataInf getZkData(DirectoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof ZkDataImpl) {
                    ZkDataImpl zkDirectoryValue = (ZkDataImpl) directObj;

                    if (name.equals(zkDirectoryValue.getName())) {

                        return zkDirectoryValue;
                    }
                }
            }
        }
        return null;
    }

    /**
     * getZkDirectory
     *
     * @param zkDirectory
     * @param name
     * @return
     * @Created 2016/9/16
     */
    protected DirectoryInf getZkDirectory(DirectoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof DirectoryInf) {
                    DirectoryInf zkDirectoryValue = (DirectoryInf) directObj;

                    if (name.equals(zkDirectoryValue.getDataName())) {

                        return zkDirectoryValue;
                    }
                }
            }
        }
        return null;
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        this.gson = gson;
    }

}
