/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.impl;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Feeds the ucore node list to a grpc channel, the way grpc-java's examples/nameresolve does: the resolver owns
 * "which nodes exist", the load balancing policy (pick_first by default) owns "which one to use", and the channel
 * owns connecting and reconnecting.
 * <p>
 * The node list changes at runtime (UcoreSender#setIpList), so every change is pushed to the live resolvers through
 * the SynchronizationContext the channel handed to them, as NameResolver.Listener2#onResult2 requires.
 */
final class UcoreNameResolverProvider extends NameResolverProvider {
    static final String SCHEME = "ucore";
    static final String TARGET = SCHEME + ":///cluster";

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreNameResolverProvider.class);

    private final int port;
    private final Set<UcoreNameResolver> resolvers = ConcurrentHashMap.newKeySet();
    private volatile List<String> ips;

    UcoreNameResolverProvider(List<String> ips, int port) {
        this.ips = Collections.unmodifiableList(new ArrayList<>(ips));
        this.port = port;
    }

    /**
     * the node list the channel is currently fed with
     */
    List<String> getIps() {
        return ips;
    }

    /**
     * called when ucore reports a new node list, every live resolver pushes it to its channel
     */
    void update(List<String> newIps) {
        this.ips = Collections.unmodifiableList(new ArrayList<>(newIps));
        for (UcoreNameResolver resolver : resolvers) {
            resolver.resolveInContext();
        }
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new UcoreNameResolver(args.getSynchronizationContext());
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    private final class UcoreNameResolver extends NameResolver {
        private final SynchronizationContext syncContext;
        private Listener2 listener;
        private boolean stopped;

        UcoreNameResolver(SynchronizationContext syncContext) {
            this.syncContext = syncContext;
        }

        @Override
        public String getServiceAuthority() {
            return "ucore-cluster";
        }

        @Override
        public void start(Listener2 newListener) {
            this.listener = newListener;
            resolvers.add(this);
            resolve();
        }

        @Override
        public void refresh() {
            resolve();
        }

        @Override
        public void shutdown() {
            stopped = true;
            resolvers.remove(this);
        }

        /**
         * start, refresh and shutdown already run in the channel's SynchronizationContext, a caller on any other
         * thread has to go through it
         */
        void resolveInContext() {
            syncContext.execute(this::resolve);
        }

        private void resolve() {
            if (listener == null || stopped) {
                return;
            }
            List<String> current = ips;
            if (current.isEmpty()) {
                listener.onError(Status.UNAVAILABLE.withDescription("no ucore ip is known"));
                return;
            }
            List<EquivalentAddressGroup> groups = new ArrayList<>(current.size());
            for (String ip : current) {
                // every address is its own EquivalentAddressGroup, as in the grpc-java example. the authority stays
                // ip:port, which is what the channel used when it was built with forAddress(ip, port)
                Attributes attrs = Attributes.newBuilder().set(EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE, ip + ":" + port).build();
                groups.add(new EquivalentAddressGroup(new InetSocketAddress(ip, port), attrs));
            }
            Status status = listener.onResult2(ResolutionResult.newBuilder().setAddressesOrError(StatusOr.fromValue(groups)).build());
            if (!status.isOk()) {
                LOGGER.warn("the channel did not accept the ucore address list " + current + ": " + status);
            }
        }
    }
}
