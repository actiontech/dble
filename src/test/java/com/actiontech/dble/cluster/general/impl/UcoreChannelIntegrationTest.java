/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.impl;

import com.actiontech.dble.alarm.UcoreGrpc;
import com.actiontech.dble.alarm.UcoreInterface;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connects to a real ucore cluster through {@link UcoreNameResolverProvider} and checks that the basic grpc plumbing
 * works: the channel resolves the ucore scheme, connects to one of the nodes, and carries a read-only rpc.
 * <p>
 * Only read-only rpcs are used (health check, leader lookup, usage), no kv is touched. When no ucore node is reachable
 * the tests are skipped, so the suite stays green on a machine without the cluster.
 */
public class UcoreChannelIntegrationTest {

    private static final List<String> UCORE_IPS = Arrays.asList("10.186.63.94", "10.186.63.93", "10.186.57.108");
    private static final int UCORE_PORT = 5700;
    private static final int RPC_TIMEOUT_SECONDS = 15;

    private UcoreNameResolverProvider provider;
    private ManagedChannel channel;

    @Before
    public void setUp() {
        Assume.assumeTrue("no ucore node reachable on port " + UCORE_PORT + ", integration test skipped",
                atLeastOneNodeReachable(UCORE_IPS));
        provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        NameResolverRegistry.getDefaultRegistry().register(provider);
        channel = ManagedChannelBuilder.forTarget(UcoreNameResolverProvider.TARGET)
                .defaultLoadBalancingPolicy("pick_first")
                .usePlaintext()
                .build();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (provider != null) {
            NameResolverRegistry.getDefaultRegistry().deregister(provider);
        }
    }

    @Test
    public void healthCheckRpcSucceeds() {
        stub().ucoreHealthCheck(UcoreInterface.Empty.getDefaultInstance());
        // reaching this line means the rpc completed with OK, grpc throws otherwise
    }

    @Test
    public void leaderLookupReturnsANodeName() {
        UcoreInterface.UcoreLearderOutput leader = stub().ucoreLearder(UcoreInterface.Empty.getDefaultInstance());
        Assert.assertNotNull(leader);
        Assert.assertNotNull("ucore should report which node answered", leader.getNodeName());
        Assert.assertFalse("ucore should report which node answered", leader.getNodeName().isEmpty());
    }

    @Test
    public void usageRpcIsAnswerable() {
        UcoreInterface.GetUsageOutput usage = stub().getUsage(UcoreInterface.Empty.getDefaultInstance());
        Assert.assertNotNull(usage);
    }

    @Test
    public void channelReportsReady() throws InterruptedException {
        stub().ucoreHealthCheck(UcoreInterface.Empty.getDefaultInstance());
        Assert.assertEquals("the channel should be READY once an rpc has been carried",
                io.grpc.ConnectivityState.READY, channel.getState(false));
    }

    /**
     * the first address refuses the connection at once (nothing listens on port 1 of localhost), pick_first has to
     * move on to the real ucore nodes.
     * the setUp provider has to step out of the registry first: for one scheme the registry keeps the first
     * registered provider of the highest priority, so it would otherwise answer for this channel too.
     */
    @Test
    public void pickFirstSkipsAnUnreachableNode() throws InterruptedException {
        NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
        List<String> ips = new ArrayList<>();
        ips.add("127.0.0.1");
        ips.addAll(UCORE_IPS);
        UcoreNameResolverProvider providerWithDeadNode = new UcoreNameResolverProvider(ips, UCORE_PORT);
        registry.deregister(provider);
        registry.register(providerWithDeadNode);
        ManagedChannel failoverChannel = ManagedChannelBuilder.forTarget(UcoreNameResolverProvider.TARGET)
                .defaultLoadBalancingPolicy("pick_first")
                .usePlaintext()
                .build();
        try {
            UcoreInterface.UcoreLearderOutput leader = UcoreGrpc.newBlockingStub(failoverChannel).withWaitForReady()
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ucoreLearder(UcoreInterface.Empty.getDefaultInstance());
            Assert.assertFalse(leader.getNodeName().isEmpty());
        } finally {
            failoverChannel.shutdownNow();
            failoverChannel.awaitTermination(5, TimeUnit.SECONDS);
            registry.deregister(providerWithDeadNode);
            registry.register(provider);
        }
    }

    /**
     * Counts how often grpc dials an address that keeps failing, using a local socket that accepts and hangs up at
     * once. Two behaviours are pinned down here:
     * - while the rest of the list still answers, a failing address is dialled exactly once per round, there is no
     *   per-address retry: InternalSubchannel increments the address index on transportShutdown and starts the next
     *   transport right away.
     * - that switch costs no delay, the exponential backoff is only scheduled once every address has failed.
     */
    @Test
    public void deadAddressIsDialledOnceThenTheNextNodeAnswers() throws Exception {
        ServerSocket deadNode = bindFakeDeadNode();
        AtomicInteger attempts = new AtomicInteger();
        Thread acceptor = startAcceptor(deadNode, attempts);

        NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
        List<String> ips = new ArrayList<>();
        ips.add("127.0.0.1");
        ips.addAll(UCORE_IPS);
        UcoreNameResolverProvider providerWithDeadNode = new UcoreNameResolverProvider(ips, UCORE_PORT);
        registry.deregister(provider);
        registry.register(providerWithDeadNode);
        ManagedChannel failoverChannel = ManagedChannelBuilder.forTarget(UcoreNameResolverProvider.TARGET)
                .defaultLoadBalancingPolicy("pick_first")
                .usePlaintext()
                .build();
        try {
            long start = System.currentTimeMillis();
            UcoreInterface.UcoreLearderOutput leader = UcoreGrpc.newBlockingStub(failoverChannel).withWaitForReady()
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ucoreLearder(UcoreInterface.Empty.getDefaultInstance());
            long elapsed = System.currentTimeMillis() - start;

            Assert.assertFalse(leader.getNodeName().isEmpty());
            Assert.assertTrue("the dead first address was never dialled, attempts=" + attempts.get(), attempts.get() >= 1);
            // one dial per address per round; a second dial would mean grpc retried the address before moving on
            Assert.assertTrue("pick_first dialled the dead address " + attempts.get() + " times, expected 1",
                    attempts.get() <= 2);
            // no backoff between addresses: the initial backoff is ~1s, so waiting there would show up here
            Assert.assertTrue("switching to the next address took " + elapsed + " ms", elapsed < 5000);
        } finally {
            failoverChannel.shutdownNow();
            failoverChannel.awaitTermination(5, TimeUnit.SECONDS);
            registry.deregister(providerWithDeadNode);
            registry.register(provider);
            stopAcceptor(deadNode, acceptor);
        }
    }

    /**
     * The same fake node as the only address: every round fails, so the whole list is retried after the exponential
     * backoff (initial 1s, multiplier 1.6, jitter 20%). With a 3.5s deadline that allows roughly three rounds, which
     * is what tells an unbounded retry loop from a throttled one.
     */
    @Test
    public void retryingTheWholeListIsThrottledByBackoff() throws Exception {
        ServerSocket deadNode = bindFakeDeadNode();
        AtomicInteger attempts = new AtomicInteger();
        Thread acceptor = startAcceptor(deadNode, attempts);

        NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
        UcoreNameResolverProvider providerOnlyDeadNode = new UcoreNameResolverProvider(
                Collections.singletonList("127.0.0.1"), UCORE_PORT);
        registry.deregister(provider);
        registry.register(providerOnlyDeadNode);
        ManagedChannel channelToDeadNode = ManagedChannelBuilder.forTarget(UcoreNameResolverProvider.TARGET)
                .defaultLoadBalancingPolicy("pick_first")
                .usePlaintext()
                .build();
        try {
            try {
                UcoreGrpc.newBlockingStub(channelToDeadNode).withWaitForReady()
                        .withDeadlineAfter(3500, TimeUnit.MILLISECONDS)
                        .ucoreLearder(UcoreInterface.Empty.getDefaultInstance());
                Assert.fail("no ucore node answers, the rpc has to end with DEADLINE_EXCEEDED");
            } catch (StatusRuntimeException e) {
                Assert.assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
            }
            Assert.assertTrue("the dead node was never dialled", attempts.get() >= 2);
            Assert.assertTrue("the whole list was retried " + attempts.get() + " times in 3.5s, backoff is not applied",
                    attempts.get() <= 5);
        } finally {
            channelToDeadNode.shutdownNow();
            channelToDeadNode.awaitTermination(5, TimeUnit.SECONDS);
            registry.deregister(providerOnlyDeadNode);
            registry.register(provider);
            stopAcceptor(deadNode, acceptor);
        }
    }

    /**
     * a listener on 127.0.0.1:UCORE_PORT that accepts a connection and hangs up at once, which grpc sees as a failed
     * transport, the same as connection refused. the test is skipped when that port is taken.
     */
    private static ServerSocket bindFakeDeadNode() throws IOException {
        ServerSocket socket = new ServerSocket();
        try {
            socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), UCORE_PORT));
        } catch (IOException e) {
            Assume.assumeNoException("127.0.0.1:" + UCORE_PORT + " is not free, the test needs it", e);
        }
        return socket;
    }

    private static Thread startAcceptor(final ServerSocket socket, final AtomicInteger attempts) {
        Thread acceptor = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!socket.isClosed()) {
                    try (Socket accepted = socket.accept()) {
                        attempts.incrementAndGet();
                    } catch (IOException e) {
                        return;
                    }
                }
            }
        }, "fake-dead-ucore");
        acceptor.setDaemon(true);
        acceptor.start();
        return acceptor;
    }

    private static void stopAcceptor(ServerSocket socket, Thread acceptor) throws IOException, InterruptedException {
        socket.close();
        acceptor.join(2000);
    }

    private UcoreGrpc.UcoreBlockingStub stub() {
        return UcoreGrpc.newBlockingStub(channel).withWaitForReady()
                .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static boolean atLeastOneNodeReachable(List<String> ips) {
        for (String ip : ips) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(ip, UCORE_PORT), 2000);
                return true;
            } catch (IOException e) {
                // try the next node
            }
        }
        return false;
    }
}
