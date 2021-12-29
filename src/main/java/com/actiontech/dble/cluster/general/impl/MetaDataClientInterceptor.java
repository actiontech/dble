package com.actiontech.dble.cluster.general.impl;

import com.actiontech.dble.config.model.SystemConfig;
import io.grpc.*;

public class MetaDataClientInterceptor implements ClientInterceptor {

    private static String UCORE_RPC_CLIENT_ID_KEY = "universe-client-id";
    private static Metadata.Key<String> dbleIdHeadKey = Metadata.Key.of(UCORE_RPC_CLIENT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(dbleIdHeadKey, "dble-" + SystemConfig.getInstance().getInstanceName());
                super.start(responseListener, headers);
            }
        };
    }
}
