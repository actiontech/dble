/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;


import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLNonTransientException;
import java.util.List;

/**
 * <p>
 * use ZK(get InstanceID from ZK) Or local file (set InstanceID) generate a sequence
 * ID :long 63 bits
 * |threadId(9)|instanceId(9)|increment(6)|current time millis(39 digits ,used for 17 years)|
 * <p/>
 * <p>
 * |threadId|instanceId|increment|current time millis|
 */
public class DistributedSequenceHandler implements Closeable, SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DistributedSequenceHandler.class);
    private static DistributedSequenceHandler instance = new DistributedSequenceHandler();

    private final long threadIdBits = 9L;
    private final long instanceIdBits = 9L;
    private final long incrementBits = 6L;
    private final long timestampBits = 39L;

    private final long incrementShift = timestampBits;
    private final long instanceIdShift = incrementShift + incrementBits;
    private volatile long instanceId;
    //The number of retries after zk generated id & 511 collision
    private final int retryCount = 5;

    private ThreadLocal<Long> threadInc = new ThreadLocal<>();
    private ThreadLocal<Long> threadLastTime = new ThreadLocal<>();
    private ThreadLocal<Long> threadID = new ThreadLocal<>();
    private long nextID = 0L;
    private static final String INSTANCE_PATH = KVPathUtil.getSequencesInstancePath();
    private volatile boolean ready = false;
    private final long startTimeMilliseconds = ClusterConfig.getInstance().sequenceStartTime();
    private long deadline = 0L;

    private CuratorFramework client;

    public static DistributedSequenceHandler getInstance() {
        return DistributedSequenceHandler.instance;
    }

    public void load(boolean isLowerCaseTableNames) {
        if (ClusterConfig.getInstance().isSequenceInstanceByZk()) {
            initializeZK();
            loadInstanceIdByZK();
        } else {
            loadInstanceIdByConfig();
        }
        this.ready = true;
        this.deadline = startTimeMilliseconds + (1L << 39);
    }

    private void loadInstanceIdByConfig() {
        this.instanceId = SystemConfig.getInstance().getInstanceId();
        long maxInstanceId = ~(-1L << instanceIdBits);
        if (instanceId > maxInstanceId || instanceId < 0) {
            throw new IllegalArgumentException(String.format("instanceId can't be greater than %d or less than 0", maxInstanceId));
        }
    }

    private void loadInstanceIdByZK() {
        int execCount = 1;
        while (true) {
            if (execCount > this.retryCount) {
                throw new RuntimeException("instanceId allocate error when using zk, reason: no available instanceId found");
            }
            try {
                List<String> nodeList = client.getChildren().forPath(INSTANCE_PATH);
                String slavePath = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).
                        forPath(INSTANCE_PATH.concat("/node"), "ready".getBytes());
                String tempInstanceId = slavePath.substring(slavePath.length() - 10);
                this.instanceId = Long.parseLong(tempInstanceId) & ((1 << instanceIdBits) - 1);
                //check if id collides
                if (checkInstanceIdCollision(nodeList)) {
                    execCount++;
                } else {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException("instanceId allocate error when using zk, reason:" + e.getMessage());
            }
        }
    }

    public void initializeZK() {
        String zkAddress = ClusterConfig.getInstance().getClusterIP();
        if (zkAddress == null) {
            throw new RuntimeException("please check clusterIP is correct in config file \"cluster.cnf\" .");
        }
        if (this.client != null) {
            this.client.close();
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
    }

    /**
     * check if id collides
     *
     * @param nodeList
     * @return
     */
    private boolean checkInstanceIdCollision(List<String> nodeList) {
        return nodeList.stream().anyMatch(e -> (Long.parseLong(e.substring(e.length() - 10)) & ((1 << instanceIdBits) - 1)) == this.instanceId);
    }

    @Override
    public long nextId(String prefixName) throws SQLNonTransientException {
        // System.out.println(instanceId);
        while (!ready) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.info("Unexpected thread interruption!");
                Thread.currentThread().interrupt();
            }
        }
        long time = System.currentTimeMillis();
        if (time >= deadline) {
            throw new SQLNonTransientException("Global sequence has reach to max limit and can generate duplicate sequences.");
        }
        Long lastTimestamp = threadLastTime.get();
        if (lastTimestamp == null) {
            if (time >= startTimeMilliseconds) {
                threadLastTime.set(time);
                lastTimestamp = time;
            } else {
                lastTimestamp = startTimeMilliseconds;
            }
        }

        if (lastTimestamp > time) {
            throw new SQLNonTransientException("Clock moved backwards.  Refusing to generate id for " +
                    (lastTimestamp - time) + " milliseconds");
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
                (a << incrementShift) | ((time - startTimeMilliseconds) & timestampMask);
    }

    private synchronized Long getNextThreadID() {
        long i = nextID;
        nextID++;
        return i;
    }

    private long blockUntilNextMillis(long time) {
        while (true) {
            if (System.currentTimeMillis() != time) {
                break;
            }
        }
        return System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(this.client);
    }
}
