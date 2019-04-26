/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sequence;

import com.actiontech.dble.route.sequence.handler.DistributedSequenceHandler;
import junit.framework.Assert;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLNonTransientException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Hash Zhang
 * @version 1.0
 * @time 00:12:05 2016/5/3
 */
@Ignore
public class DistributedSequenceHandlerTest {
    TestingServer testingServer = null;
    DistributedSequenceHandler distributedSequenceHandler[];

    @Before
    public void initialize() throws Exception {
        distributedSequenceHandler = new DistributedSequenceHandler[16];
        testingServer = new TestingServer();
        testingServer.start();
        for (int i = 0; i < 16; i++) {
            distributedSequenceHandler[i] = new DistributedSequenceHandler();
            distributedSequenceHandler[i].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[i].nextId("");
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUniqueInstanceID() throws Exception {
        Set<Long> idSet = new HashSet<>();
        for (int i = 0; i < 16; i++) {
            idSet.add(distributedSequenceHandler[i].getInstanceId());
        }
        Assert.assertEquals(idSet.size(), 16);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUniqueID() throws Exception {
        final ConcurrentHashMap<Long, String> idSet = new ConcurrentHashMap<>();
        Thread thread[] = new Thread[10];
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        for (int k = 0; k < 16; k++) {
                            long id = 0;
                            try {
                                id = distributedSequenceHandler[k].nextId("");
                            } catch (SQLNonTransientException e) {
                                e.printStackTrace();
                            }
                            idSet.put(id, "");
                        }
                    }

                }
            };
            thread[i].start();
        }
        for (int i = 0; i < 10; i++) {
            thread[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println("Time elapsed:" + (double) (end - start) / 1000.0 + "s");
        System.out.println("ID/s:" + (((double) idSet.size()) / ((double) (end - start) / 1000.0)));
        Assert.assertEquals(idSet.size(), 16000);
    }

    /**
     * testFailOver
     *
     * @throws Exception
     */
    @Test
    public void testFailOver() {
        Set<Long> idSet = new HashSet<>();
        try {
            int leader = failLeader(17);
            System.out.println("*** When a leader is offline,curator will throw an expected exception. ***:");
            for (int i = 0; i < 16; i++) {
                if (i == leader) {
                    System.out.println("Node [" + i + "] used to be leader");
                    continue;
                }
                distributedSequenceHandler[i].nextId("");
                System.out.println("Node [" + i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership());
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
            Assert.assertEquals(idSet.size(), 15);
            idSet = new HashSet<>();
            int leader2 = failLeader(leader);
            System.out.println("*** When two leaders are offline,curator will throw an expected exception. ***:");
            for (int i = 0; i < 16; i++) {
                if (i == leader || i == leader2) {
                    System.out.println("Node [" + i + " used to be leader");
                    continue;
                }
                distributedSequenceHandler[i].nextId("");
                System.out.println("Node [" + i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership());
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
            Assert.assertEquals(idSet.size(), 14);

            idSet = new HashSet<>();
            distributedSequenceHandler[leader] = new DistributedSequenceHandler();
            distributedSequenceHandler[leader].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[leader].nextId("");
            distributedSequenceHandler[leader2] = new DistributedSequenceHandler();
            distributedSequenceHandler[leader2].initializeZK(testingServer.getConnectString());
            distributedSequenceHandler[leader2].nextId("");
            System.out.println("add two new nodes");
            for (int i = 0; i < 16; i++) {
                System.out.println("Node [" + i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership());
                System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
                idSet.add(distributedSequenceHandler[i].getInstanceId());
            }
        } catch (Exception e) {

        } finally {
            Assert.assertEquals(idSet.size(), 16);
        }

    }

    private int failLeader(int p) {
        int leader = 0, follower = 0;
        for (int i = 0; i < 16; i++) {
            if (i == p) {
                continue;
            }
            if (distributedSequenceHandler[i].getLeaderSelector().hasLeadership()) {
                leader = i;
            } else {
                follower = i;
            }
            System.out.println("Node [" + i + "]is leader:" + distributedSequenceHandler[i].getLeaderSelector().hasLeadership());
            System.out.println(" InstanceID:" + distributedSequenceHandler[i].getInstanceId());
        }
        try {
            distributedSequenceHandler[leader].close();
        } catch (IOException e) {
        }

        while (true) {
            follower++;
            if (follower >= 16) {
                follower = 0;
            }
            if (follower == leader || follower == p) {
                continue;
            }
            if (distributedSequenceHandler[follower].getLeaderSelector().hasLeadership()) {
                break;
            }
        }
        return leader;
    }

}
