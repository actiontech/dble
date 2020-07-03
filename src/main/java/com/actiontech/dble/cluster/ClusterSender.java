/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.general.bean.KvBean;

import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2019/3/11.
 */
public interface ClusterSender {

    /**
     * general config init of clusterSender
     * There are several task may be start
     * 1 init the config
     * 2 start customized connection controller
     *
     */
    void initCluster();


    /**
     * put KV into cluster
     *
     * @param path set path
     * @param value set value
     * @throws Exception io or net error
     */
    void setKV(String path, String value) throws Exception;

    /**
     * get KV from cluster
     *
     * @param path get path
     * @return key value pair
     */
    KvBean getKV(String path);

    /**
     * return all the KV under the path
     * <p>
     * NOTICE: the KV in result list should be full path
     *
     * @param path get path
     * @return all key value tree
     */
    List<KvBean> getKVPath(String path);

    /**
     * clean/delete all the KV under the path
     * NOTICE: path itself also need cleaned
     *
     * @param path delete path
     */
    void cleanPath(String path);

    /**
     * clean/delete single KV
     *
     * @param path  delete path
     */
    void cleanKV(String path);

    /**
     * createSelfTempNode for sync status
     * @param path create path
     * @param value content
     * @throws Exception  io or net error
     */
    void createSelfTempNode(String path, String value) throws Exception;

    /**
     * writeDirectly xml and sequence to cluster and change status
     * @throws Exception  io or net error
     */
    void writeConfToCluster() throws Exception;

    /**
     * createDistributeLock
     * @param path lock path
     * @param value lock value
     * @return lock
     */
    DistributeLock createDistributeLock(String path, String value);
    /**
     * createDistributeLock with maxErrorCnt
     * @param path lock path
     * @param value lock value
     * @return lock
     */
    DistributeLock createDistributeLock(String path, String value, int maxErrorCnt);

    /**
     * get online nodes
     * @return online node and info
     */
    Map<String, String> getOnlineMap();

    /**
     * forceResumePause sharding node
     * @throws Exception   io or net error
     */
    void forceResumePause() throws Exception;
}
