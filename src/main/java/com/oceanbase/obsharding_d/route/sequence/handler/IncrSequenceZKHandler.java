/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.sequence.handler;


import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.config.ConfigFileName;
import com.oceanbase.obsharding_d.config.converter.SequenceConverter;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.route.util.PropertiesUtil;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * zookeeper IncrSequenceZKHandler
 * file:sequence_conf.properties
 * `DB`.`TABLE`.MINID minID for a thread
 * `DB`.`TABLE`.MAXID maxID for a thread
 * `DB`.`TABLE`.CURID curID for a thread
 * the properties in file is useful the first thread,the other thread/process will read from ZK
 *
 * @author Hash Zhang
 * @version 1.0
 * 23:35 2016/5/6
 */
public class IncrSequenceZKHandler extends IncrSequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceZKHandler.class);
    private static final String PATH = KVPathUtil.getSequencesIncrPath() + "/";
    private static final String LOCK = "/lock";
    private static final String SEQ = "/seq";

    private ThreadLocal<Map<String, Map<String, String>>> tableParaValMapThreadLocal = new ThreadLocal<>();
    private Set<Thread> threadList = new HashSet<>();
    private Set<Thread> removeThreadList = new HashSet<>();

    private CuratorFramework client;
    private ThreadLocal<InterProcessSemaphoreMutex> interProcessSemaphoreMutexThreadLocal = new ThreadLocal<>();
    private Properties props;

    public synchronized void load(RawJson sequenceJson, Set<String> currentShardingNodes) {
        if (sequenceJson != null) {
            // load cluster properties
            SequenceConverter sequenceConverter = new SequenceConverter();
            this.props = sequenceConverter.jsonToProperties(sequenceJson);
        } else {
            // load local properties
            this.props = PropertiesUtil.loadProps(ConfigFileName.SEQUENCE_FILE_NAME);
        }
        String zkAddress = ClusterConfig.getInstance().getClusterIP();
        if (zkAddress == null) {
            throw new RuntimeException("please check ClusterIP is correct in config file \"cluster.cnf\" .");
        }
        try {
            initializeZK(this.props, zkAddress);
        } catch (Exception e) {
            LOGGER.warn("Error caught while initializing ZK:" + e.getCause());
        }
    }

    public void tryLoad(RawJson sequenceJson, Set<String> currentShardingNodes) {
        load(sequenceJson, currentShardingNodes);
        if (client != null) {
            client.close();
        }
    }

    private void threadLocalLoad() throws Exception {
        Enumeration<?> enu = this.props.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            if (key.endsWith(KEY_MIN_NAME)) {
                handle(key);
            }
        }
    }

    public void initializeZK(Properties properties, String zkAddress) throws Exception {
        if (this.client != null) {
            this.client.close();
        }
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        this.props = properties;
        this.tableParaValMapThreadLocal.remove();
        this.interProcessSemaphoreMutexThreadLocal.remove();
        this.removeThreadList.addAll(threadList);
        this.threadList.clear();
        this.removeThreadList.remove(Thread.currentThread());
    }

    private void handle(String key) throws Exception {
        String table = key.substring(0, key.indexOf(KEY_MIN_NAME));
        InterProcessSemaphoreMutex interProcessSemaphoreMutex = interProcessSemaphoreMutexThreadLocal.get();
        if (interProcessSemaphoreMutex == null) {
            interProcessSemaphoreMutex = new InterProcessSemaphoreMutex(client, PATH + table + SEQ + LOCK);
            interProcessSemaphoreMutexThreadLocal.set(interProcessSemaphoreMutex);
        }
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            tableParaValMap = new HashMap<>();
            tableParaValMapThreadLocal.set(tableParaValMap);
        }
        Map<String, String> paraValMap = tableParaValMap.get(table);
        if (paraValMap == null) {
            paraValMap = new ConcurrentHashMap<>();
            tableParaValMap.put(table, paraValMap);

            String seqPath = PATH + table + SEQ;

            Stat stat = this.client.checkExists().forPath(seqPath);

            if (stat == null || (stat.getDataLength() == 0)) {
                paraValMap.put(table + KEY_MIN_NAME, this.props.getProperty(key));
                paraValMap.put(table + KEY_MAX_NAME, this.props.getProperty(table + KEY_MAX_NAME));
                paraValMap.put(table + KEY_CUR_NAME, this.props.getProperty(table + KEY_CUR_NAME));
                try {
                    String val = this.props.getProperty(table + KEY_MIN_NAME);
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(PATH + table + SEQ, val.getBytes());
                } catch (Exception e) {
                    LOGGER.debug("Node exists! Maybe other instance is initializing!");
                }
            }
            fetchNextPeriod(table);
        }
    }

    @Override
    public Object[] getParaValMap(String prefixName) {
        if (this.removeThreadList.remove(Thread.currentThread())) {
            this.interProcessSemaphoreMutexThreadLocal.remove();
            this.tableParaValMapThreadLocal.remove();
        }
        if (props.entrySet().isEmpty()) return null;
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            try {
                threadLocalLoad();
            } catch (Exception e) {
                LOGGER.warn("Error caught while loading configuration within current thread:" + e.getCause());
            }
            tableParaValMap = tableParaValMapThreadLocal.get();
            threadList.add(Thread.currentThread());
        }
        return matching(prefixName, tableParaValMap);
    }

    private Object[] matching(String key, Map<String, Map<String, String>> map) {
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            Optional<Map.Entry<String, Map<String, String>>> result = map.entrySet().stream().filter(m -> m.getKey().equalsIgnoreCase(key)).findFirst();
            if (result.isPresent())
                return new Object[]{result.get().getKey(), result.get().getValue()};
        } else {
            if (map.containsKey(key))
                return new Object[]{key, map.get(key)};
        }
        return null;
    }

    @Override
    public Boolean fetchNextPeriod(String prefixName) {
        InterProcessSemaphoreMutex interProcessSemaphoreMutex = interProcessSemaphoreMutexThreadLocal.get();
        try {
            if (interProcessSemaphoreMutex == null) {
                throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
            }
            interProcessSemaphoreMutex.acquire();
            try {
                Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
                if (tableParaValMap == null) {
                    throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
                }
                Map<String, String> paraValMap = tableParaValMap.get(prefixName);
                if (paraValMap == null) {
                    throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
                }

                if (paraValMap.get(prefixName + KEY_MAX_NAME) == null) {
                    paraValMap.put(prefixName + KEY_MAX_NAME, this.props.getProperty(prefixName + KEY_MAX_NAME));
                }
                if (paraValMap.get(prefixName + KEY_MIN_NAME) == null) {
                    paraValMap.put(prefixName + KEY_MIN_NAME, this.props.getProperty(prefixName + KEY_MIN_NAME));
                }
                if (paraValMap.get(prefixName + KEY_CUR_NAME) == null) {
                    paraValMap.put(prefixName + KEY_CUR_NAME, this.props.getProperty(prefixName + KEY_CUR_NAME));
                }

                long period = Long.parseLong(paraValMap.get(prefixName + KEY_MAX_NAME)) - Long.parseLong(paraValMap.get(prefixName + KEY_MIN_NAME));
                long now = Long.parseLong(new String(client.getData().forPath(PATH + prefixName + SEQ)));
                client.setData().forPath(PATH + prefixName + SEQ, ((now + period + 1) + "").getBytes());

                paraValMap.put(prefixName + KEY_MIN_NAME, (now) + "");
                paraValMap.put(prefixName + KEY_MAX_NAME, (now + period) + "");
                paraValMap.put(prefixName + KEY_CUR_NAME, (now) - 1 + "");
            } catch (Exception e) {
                LOGGER.warn("Error caught while updating period from ZK:" + e.getCause());
            } finally {
                interProcessSemaphoreMutex.release();
            }
        } catch (Exception e) {
            LOGGER.error("Error caught while use distributed lock:" + e.getCause());
        }
        return true;
    }

    @Override
    public Boolean updateCurIDVal(String prefixName, Long val) {
        Map<String, Map<String, String>> tableParaValMap = tableParaValMapThreadLocal.get();
        if (tableParaValMap == null) {
            throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
        }
        Map<String, String> paraValMap = tableParaValMap.get(prefixName);
        if (paraValMap == null) {
            throw new IllegalStateException("IncrSequenceZKHandler should be loaded first!");
        }
        paraValMap.put(prefixName + KEY_CUR_NAME, val + "");
        return true;
    }

    @Override
    public synchronized long nextId(String prefixName, @Nullable FrontendService frontendService) {
        if (frontendService != null) {
            frontendService.getClusterDelayService().markDoingOrDelay(true);
        }
        return super.nextId(prefixName, frontendService);
    }

    public synchronized void detach() throws Exception {
        if (this.client != null) {
            this.client.close();
        }
    }

    public synchronized void attach() throws Exception {
        String zkAddress = ClusterConfig.getInstance().getClusterIP();
        if (zkAddress == null) {
            throw new RuntimeException("please check ClusterIP is correct in config file \"cluster.cnf\" .");
        }
        initializeZK(this.props, zkAddress);

    }

}
