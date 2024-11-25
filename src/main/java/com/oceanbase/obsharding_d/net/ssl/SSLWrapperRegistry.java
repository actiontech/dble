/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.ssl;

import com.google.common.collect.Maps;

import java.util.Map;

public final class SSLWrapperRegistry {

    protected static final Map<Integer, OpenSSLWrapper> SSL_CONTEXT_REGISTRY = Maps.newHashMap();

    static {
        register(OpenSSLWrapper.PROTOCOL, new OpenSSLWrapper());
        register(GMSslWrapper.PROTOCOL, new GMSslWrapper());
    }

    private SSLWrapperRegistry() {
    }

    static void register(int protocol, OpenSSLWrapper sslWrapper) {
        SSL_CONTEXT_REGISTRY.put(protocol, sslWrapper);
    }

    public static OpenSSLWrapper getInstance(int protocol) {
        return SSL_CONTEXT_REGISTRY.get(protocol);
    }

    public static void init() {
        //init & remove incomplete configuration
        SSL_CONTEXT_REGISTRY.entrySet().removeIf(sslContext ->
                !sslContext.getValue().initContext());
    }

    public enum SSLProtocol {
        OPEN_SSL(OpenSSLWrapper.PROTOCOL, OpenSSLWrapper.NAME), GM_SSL(GMSslWrapper.PROTOCOL, GMSslWrapper.NAME);

        private final int id;
        private final String name;

        SSLProtocol(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public static String nameOf(int id) {
            for (SSLProtocol sslProtocol : SSLProtocol.values()) {
                if (sslProtocol.id == id) {
                    return sslProtocol.name;
                }
            }
            return OPEN_SSL.name;
        }
    }
}
