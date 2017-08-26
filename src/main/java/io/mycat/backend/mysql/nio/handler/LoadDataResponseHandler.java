package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;

/**
 * Created by nange on 2015/3/31.
 */
public interface LoadDataResponseHandler {
    void requestDataResponse(byte[] row, BackendConnection conn);
}
