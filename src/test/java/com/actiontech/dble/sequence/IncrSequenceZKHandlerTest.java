/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sequence;

import com.actiontech.dble.route.sequence.handler.IncrSequenceZKHandler;
import com.actiontech.dble.route.util.PropertiesUtil;
import junit.framework.Assert;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * zookeeper
 * 60 processors,20 thread in every 20 thread.every thead called 50 times
 * default GLOBAL.MINID=1
 * default GLOBAL.MAXID=10
 * get GLOBAL.MINID-GLOBAL.MAXID(9) every time
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 23:35 2016/5/6
 */
public class IncrSequenceZKHandlerTest {
    private static final int MAX_CONNECTION = 5;
    private static final int threadCount = 5;
    private static final int LOOP = 5;
    TestingServer testingServer = null;
    IncrSequenceZKHandler incrSequenceZKHandler[];
    ConcurrentSkipListSet<Long> results;

    @Before
    public void initialize() throws Exception {
        testingServer = new TestingServer();
        testingServer.start();
        incrSequenceZKHandler = new IncrSequenceZKHandler[MAX_CONNECTION];
        results = new ConcurrentSkipListSet();
    }

    @Test
    public void testCorrectnessAndEfficiency() throws InterruptedException {
        final Thread threads[] = new Thread[MAX_CONNECTION];
        for (int i = 0; i < MAX_CONNECTION; i++) {
            final int a = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    incrSequenceZKHandler[a] = new IncrSequenceZKHandler();
                    Properties props = PropertiesUtil.loadProps("sequence_conf.properties");
                    try {
                        incrSequenceZKHandler[a].initializeZK(props, testingServer.getConnectString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Thread threads[] = new Thread[threadCount];
                    for (int j = 0; j < threadCount; j++) {
                        threads[j] = new Thread() {
                            @Override
                            public void run() {
                                for (int k = 0; k < LOOP; k++) {
                                    long key = incrSequenceZKHandler[a].nextId("GLOBAL");
                                    results.add(key);
                                }
                            }
                        };
                        threads[j].start();
                    }
                    for (int j = 0; j < threadCount; j++) {
                        try {
                            threads[j].join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };

        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < MAX_CONNECTION; i++) {
            threads[i].start();
        }
        for (int i = 0; i < MAX_CONNECTION; i++) {
            threads[i].join();
        }
        long end = System.currentTimeMillis();
        Assert.assertEquals(MAX_CONNECTION * LOOP * threadCount, results.size());
        //        Assert.assertTrue(results.pollLast().equals(MAX_CONNECTION * LOOP * threadCount + 1L));
        //        Assert.assertTrue(results.pollFirst().equals(2L));
        System.out.println("Time elapsed:" + ((double) (end - start + 1) / 1000.0) + "s\n TPS:" + ((double) (MAX_CONNECTION * LOOP * threadCount) / (double) (end - start + 1) * 1000.0) + "/s");
    }
}
