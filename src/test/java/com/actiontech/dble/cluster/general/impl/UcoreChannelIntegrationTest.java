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
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
