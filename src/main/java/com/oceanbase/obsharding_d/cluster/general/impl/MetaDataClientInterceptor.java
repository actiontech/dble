/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.impl;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import io.grpc.*;

public class MetaDataClientInterceptor implements ClientInterceptor {

    private static String UCORE_RPC_CLIENT_ID_KEY = "universe-client-id";
    private static Metadata.Key<String> OBsharding_DIdHeadKey = Metadata.Key.of(UCORE_RPC_CLIENT_ID_KEY, Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(OBsharding_DIdHeadKey, "OBsharding-D-" + SystemConfig.getInstance().getInstanceName());
                super.start(responseListener, headers);
            }
        };
    }
}
