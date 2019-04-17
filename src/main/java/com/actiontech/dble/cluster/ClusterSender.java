package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.bean.ClusterAlertBean;
import com.actiontech.dble.cluster.bean.SubscribeRequest;
import com.actiontech.dble.cluster.bean.SubscribeReturnBean;

import java.util.List;
import java.util.Properties;

/**
 * Created by szf on 2019/3/11.
 */
public interface ClusterSender {

    /**
     * only init the connection preperties for the clusterSender
     * mainly used by shell to upload xml config
     * do not start any Thread in this function!
     *
     * @param ucoreProperties
     */
    void initConInfo(Properties ucoreProperties);

    /**
     * general config init of clusterSender
     * There are several task may be start
     * 1 init the config
     * 2 start customized connection controller
     *
     * @param properties
     */
    void initCluster(Properties properties);

    /**
     * lock a path,so that other DbleServer in cluster can not hold the path in the same time
     * return sessionId of the lock,if the return String is not null or "" means lock success
     * the lock value is also import ,the lock also need to be regarded as a KV
     * <p>
     * NOTICE: the lock should only influences it self,the child path should be available to other DbleServer to write
     *
     * @param path
     * @param value
     * @return
     * @throws Exception
     */
    String lock(String path, String value) throws Exception;


    /**
     * use the locked path and sessionId to unlock a path
     * and the KV of the lock should be removed
     *
     * @param key
     * @param sessionId
     */
    void unlockKey(String key, String sessionId);

    /**
     * put KV into cluster
     *
     * @param path
     * @param value
     * @throws Exception
     */
    void setKV(String path, String value) throws Exception;

    /**
     * get KV from cluster
     *
     * @param path
     * @return
     */
    KvBean getKV(String path);

    /**
     * return all the KV under the path
     * <p>
     * NOTICE: the KV in result list should be full path
     *
     * @param path
     * @return
     */
    List<KvBean> getKVPath(String path);

    /**
     * clean/delete all the KV under the path
     * NOTICE: path itself also need cleaned
     *
     * @param path
     */
    void cleanPath(String path);

    /**
     * clean/delete single KV
     *
     * @param path
     */
    void cleanKV(String path);

    SubscribeReturnBean subscribeKvPrefix(SubscribeRequest request) throws Exception;

    /**
     * alert something into cluster
     *
     * @param alert
     */
    void alert(ClusterAlertBean alert);

    /**
     * notify cluster some alert is resolved
     *
     * @param alert
     * @return
     */
    boolean alertResolve(ClusterAlertBean alert);

    /**
     * check if the cluster config is complete,if not throw a RunTimeException
     *
     * @param properties
     */
    void checkClusterConfig(Properties properties);
}
