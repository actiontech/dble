/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.impl;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverRegistry;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit test of {@link UcoreNameResolverProvider}. It only checks what the resolver hands to the channel (addresses,
 * authority, update and error propagation), no connection is opened: a channel connects on the first rpc, not when the
 * resolver produces addresses.
 */
public class UcoreNameResolverProviderTest {

    private static final List<String> UCORE_IPS = Arrays.asList("10.186.63.94", "10.186.63.93", "10.186.57.108");
    private static final int UCORE_PORT = 5700;

    @Test
    public void providerBindsToTheUcoreScheme() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        Assert.assertEquals("ucore", provider.getDefaultScheme());
        Assert.assertEquals("ucore:///cluster", UcoreNameResolverProvider.TARGET);
    }

    @Test
    public void providerCanBeRegisteredTheWayInitchannelDoesIt() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        NameResolverRegistry registry = NameResolverRegistry.getDefaultRegistry();
        // register throws IllegalArgumentException when isAvailable() is false, which is the check UcoreSender.initChannel relies on
        registry.register(provider);
        try {
            Assert.assertNotNull("no resolver for the ucore scheme after registering the provider",
                    provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args()));
        } finally {
            registry.deregister(provider);
        }
    }

    @Test
    public void resolverReportsEveryUcoreIpAsItsOwnAddressGroup() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        NameResolver resolver = provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args());
        Assert.assertEquals("ucore-cluster", resolver.getServiceAuthority());

        RecordingListener listener = new RecordingListener();
        resolver.start(listener);

        Assert.assertEquals("one result is expected on start", 1, listener.results.size());
        List<EquivalentAddressGroup> groups = listener.results.get(0);
        Assert.assertEquals("every ip needs its own equivalent address group", UCORE_IPS.size(), groups.size());
        for (int i = 0; i < UCORE_IPS.size(); i++) {
            String ip = UCORE_IPS.get(i);
            Assert.assertEquals(1, groups.get(i).getAddresses().size());
            InetSocketAddress address = (InetSocketAddress) groups.get(i).getAddresses().get(0);
            Assert.assertEquals(ip, address.getHostString());
            Assert.assertEquals(UCORE_PORT, address.getPort());
            // the channel is built with forAddress(ip, port), so the authority has to stay ip:port
            Assert.assertEquals(ip + ":" + UCORE_PORT,
                    groups.get(i).getAttributes().get(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE));
        }
    }

    @Test
    public void nodeListChangeIsPushedToEveryStartedResolver() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        RecordingListener first = new RecordingListener();
        RecordingListener second = new RecordingListener();
        provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args()).start(first);
        provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args()).start(second);

        provider.update(Collections.singletonList("10.186.63.93"));

        Assert.assertEquals(2, first.results.size());
        Assert.assertEquals(2, second.results.size());
        List<EquivalentAddressGroup> latest = second.results.get(1);
        Assert.assertEquals(1, latest.size());
        Assert.assertEquals("10.186.63.93", ((InetSocketAddress) latest.get(0).getAddresses().get(0)).getHostString());
    }

    @Test
    public void emptyNodeListIsReportedAsUnavailable() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        RecordingListener listener = new RecordingListener();
        provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args()).start(listener);

        provider.update(Collections.<String>emptyList());

        Assert.assertEquals("an empty ip list must not be handed to the channel", 1, listener.results.size());
        Assert.assertEquals(1, listener.errors.size());
        Assert.assertEquals(Status.Code.UNAVAILABLE, listener.errors.get(0).getCode());
    }

    @Test
    public void shutDownResolverStopsReceivingUpdates() {
        UcoreNameResolverProvider provider = new UcoreNameResolverProvider(UCORE_IPS, UCORE_PORT);
        NameResolver resolver = provider.newNameResolver(URI.create(UcoreNameResolverProvider.TARGET), args());
        RecordingListener listener = new RecordingListener();
        resolver.start(listener);

        resolver.shutdown();
        provider.update(Collections.singletonList("10.186.63.94"));

        Assert.assertEquals("a shut-down resolver must not be fed new addresses", 1, listener.results.size());
        Assert.assertEquals(0, listener.errors.size());
    }

    /**
     * a real SynchronizationContext with the default uncaught exception handler. it runs queued tasks inline on the
     * calling thread (execute == executeLater + drain), which is what makes update() observable right after the call.
     */
    private static NameResolver.Args args() {
        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable error) {
                throw new IllegalStateException("unexpected failure in the resolver synchronization context", error);
            }
        };
        return NameResolver.Args.newBuilder()
                .setDefaultPort(UCORE_PORT)
                .setSynchronizationContext(new SynchronizationContext(handler))
                .setProxyDetector(new ProxyDetector() {
                    @Override
                    public ProxiedSocketAddress proxyFor(SocketAddress targetServerAddress) {
                        return null;
                    }
                })
                .setServiceConfigParser(new NameResolver.ServiceConfigParser() {
                    @Override
                    public NameResolver.ConfigOrError parseServiceConfig(Map<String, ?> rawServiceConfig) {
                        return NameResolver.ConfigOrError.fromError(Status.UNIMPLEMENTED);
                    }
                })
                .build();
    }

    /**
     * records what the resolver pushes. onResult2 is not overridden, so the default implementation (delegate to
     * onResult and report success) is exercised as well.
     */
    private static final class RecordingListener extends NameResolver.Listener2 {
        private final List<List<EquivalentAddressGroup>> results = new ArrayList<>();
        private final List<Status> errors = new ArrayList<>();

        @Override
        public void onResult(NameResolver.ResolutionResult resolutionResult) {
            StatusOr<List<EquivalentAddressGroup>> addresses = resolutionResult.getAddressesOrError();
            Assert.assertTrue("expected an address list, got " + addresses.getStatus(), addresses.hasValue());
            results.add(addresses.getValue());
        }

        @Override
        public void onError(Status error) {
            errors.add(error);
        }
    }
}
