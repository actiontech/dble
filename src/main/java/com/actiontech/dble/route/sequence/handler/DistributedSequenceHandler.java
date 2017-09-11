/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;


import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.route.util.PropertiesUtil;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Deprecated:
 * <p>
 * use ZK(get InstanceID from ZK) Or local file (set InstanceID) generate a sequence
 * ID :long 63 bits
 * |threadId(9)|instanceId(5)|clusterId(4)|increment(6)|current time millis(39 digits ,used for 17 years)|
 * <p/>
 * local file:sequence_distributed_conf.properties
 * set :INSTANCEID=ZK then the INSTANCEIDwill generated from zk
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:08:03 2016/5/3
 * <p>
 * Now:
 * <p>
 * clusterId 4bits
 * <p>
 * |threadId|instanceId|clusterId|increment|current time millis|
 */
public class DistributedSequenceHandler extends LeaderSelectorListenerAdapter implements Closeable, SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DistributedSequenceHandler.class);
    private static final String SEQUENCE_DB_PROPS = "sequence_distributed_conf.properties";
    private static DistributedSequenceHandler instance = new DistributedSequenceHandler();

    private final long threadIdBits = 9L;
    private final long instanceIdBits = 5L;
    private final long clusterIdBits = 4L;
    private final long incrementBits = 6L;
    private final long timestampBits = 39L;

    private final long incrementShift = timestampBits;
    private final long clusterIdShift = incrementShift + incrementBits;
    private final long instanceIdShift = clusterIdShift + clusterIdBits;

    private final long maxinstanceId = 1L << instanceIdBits;

    private volatile long instanceId;
    private long clusterId;

    private ThreadLocal<Long> threadInc = new ThreadLocal<>();
    private ThreadLocal<Long> threadLastTime = new ThreadLocal<>();
    private ThreadLocal<Long> threadID = new ThreadLocal<>();
    private long nextID = 0L;
    private static final String PATH = KVPathUtil.getSequencesPath();
    private static final String INSTANCE_PATH = KVPathUtil.getSequencesInstancePath();

    private int[] mark;
    private volatile boolean isLeader = false;
    private volatile String slavePath;
    private volatile boolean ready = false;

    private CuratorFramework client;

    private LeaderSelector leaderSelector;

    private final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService leaderExecutor;

    public static DistributedSequenceHandler getInstance() {
        return DistributedSequenceHandler.instance;
    }

    public LeaderSelector getLeaderSelector() {
        return leaderSelector;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public void load() {
        // load sequnce properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS);
        if ("ZK".equals(props.getProperty("INSTANCEID"))) {
            initializeZK(ZkConfig.getInstance().getZkURL());
        } else {
            this.instanceId = Long.parseLong(props.getProperty("INSTANCEID"));
            this.ready = true;
        }
        this.clusterId = Long.parseLong(props.getProperty("CLUSTERID"));
        long maxclusterId = 1L << clusterIdBits;
        if (clusterId > maxclusterId || clusterId < 0) {
            throw new IllegalArgumentException(String.format("cluster Id can't be greater than %d or less than 0", clusterId));
        }
        if (instanceId > maxinstanceId || instanceId < 0) {
            throw new IllegalArgumentException(String.format("instance Id can't be greater than %d or less than 0", instanceId));
        }
    }

    public void initializeZK(String zkAddress) {
        if (zkAddress == null) {
            throw new RuntimeException("please check zkURL is correct in config file \"myid.prperties\" .");
        }
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        try {
            if (client.checkExists().forPath(INSTANCE_PATH) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(INSTANCE_PATH);
            }
        } catch (Exception e) {
            throw new RuntimeException("create instance path " + INSTANCE_PATH + "error", e);
        }
        this.leaderSelector = new LeaderSelector(client, KVPathUtil.getSequencesLeaderPath(), this);
        this.leaderSelector.autoRequeue();
        this.leaderSelector.start();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    while (leaderSelector.getLeader() == null) {
                        Thread.currentThread().yield();
                    }
                    if (!leaderSelector.hasLeadership()) {
                        isLeader = false;
                        if (slavePath != null && client.checkExists().forPath(slavePath) != null) {
                            return;
                        }
                        slavePath = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).
                                forPath(PATH.concat("/instance/node"), "ready".getBytes());
                        while ("ready".equals(new String(client.getData().forPath(slavePath)))) {
                            Thread.currentThread().yield();
                        }
                        instanceId = Long.parseLong(new String(client.getData().forPath(slavePath)));
                        ready = true;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Caught exception while handling zk!", e);
                }
            }
        };
        long selfCheckPeriod = 10L;
        timerExecutor.scheduleAtFixedRate(runnable, 1L, selfCheckPeriod, TimeUnit.SECONDS);
    }

    @Override
    public long nextId(String prefixName) {
        // System.out.println(instanceId);
        while (!ready) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.warn("Unexpected thread interruption!");
                Thread.currentThread().interrupt();
            }
        }
        long time = System.currentTimeMillis();
        if (threadLastTime.get() == null) {
            threadLastTime.set(time);
        }
        if (threadInc.get() == null) {
            threadInc.set(0L);
        }
        if (threadID.get() == null) {
            threadID.set(getNextThreadID());
        }
        long a = threadInc.get();
        long maxIncrement = 1L << incrementBits;
        if ((a + 1L) >= maxIncrement) {
            if (threadLastTime.get() == time) {
                time = blockUntilNextMillis(time);
            }
            threadInc.set(0L);
        } else {
            threadInc.set(a + 1L);
        }
        threadLastTime.set(time);
        long maxThreadId = 1L << threadIdBits;
        long threadIdShift = instanceIdShift + instanceIdBits;
        long timestampMask = (1L << timestampBits) - 1L;
        return (((threadID.get() % maxThreadId) << threadIdShift)) | (instanceId << instanceIdShift) |
                (clusterId << clusterIdShift) | (a << incrementShift) | (time & timestampMask);
    }

    private synchronized Long getNextThreadID() {
        long i = nextID;
        nextID++;
        return i;
    }

    private long blockUntilNextMillis(long time) {
        while (System.currentTimeMillis() == time) {
            //block
        }
        return System.currentTimeMillis();
    }

    @Override
    public void stateChanged(CuratorFramework framework, ConnectionState newState) {
        if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
            this.isLeader = false;
            leaderExecutor.shutdownNow();
            throw new CancelLeadershipException();
        }
    }

    @Override
    public void takeLeadership(final CuratorFramework curatorFramework) {
        this.isLeader = true;
        this.instanceId = 1;
        this.ready = true;
        this.mark = new int[(int) maxinstanceId];
        List<String> children = null;
        try {
            if (this.slavePath != null) {
                client.delete().forPath(slavePath);
            }
            if (client.checkExists().forPath(INSTANCE_PATH) != null) {
                children = client.getChildren().forPath(INSTANCE_PATH);
            }
            if (children != null) {
                for (String child : children) {
                    String data = new String(client.getData().forPath(INSTANCE_PATH.concat("/").concat(child)));
                    if (!"ready".equals(data)) {
                        mark[Integer.parseInt(data)] = 1;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Caught exception while handling zk!", e);
        }

        leaderExecutor = Executors.newSingleThreadScheduledExecutor();
        leaderExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!client.isStarted()) {
                        Thread.currentThread().yield();
                    }
                    List<String> children = client.getChildren().forPath(INSTANCE_PATH);
                    int[] mark2 = new int[(int) maxinstanceId];
                    for (String child : children) {
                        String data = new String(client.getData().forPath(PATH.concat("/instance/" + child)));
                        if ("ready".equals(data)) {
                            int i = nextFree();
                            client.setData().forPath(INSTANCE_PATH.concat("/").concat(child), ("" + i).getBytes());
                            mark2[i] = 1;
                        } else {
                            mark2[Integer.parseInt(data)] = 1;
                        }
                    }
                    mark = mark2;
                } catch (Exception e) {
                    LOGGER.warn("Caught exception while handling zk!", e);
                }
            }
        }, 0L, 3L, TimeUnit.SECONDS);

        while (true) {
            Thread.currentThread().yield();
        }
    }

    private int nextFree() {
        for (int i = 0; i < mark.length; i++) {
            if (i == 1) {
                /* myself, please check in takeLeadership(): this.instanceId = 1; */
                continue;
            }
            if (mark[i] != 1) {
                mark[i] = 1;
                return i;
            }
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(this.leaderSelector);
        CloseableUtils.closeQuietly(this.client);
    }
}
