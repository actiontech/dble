package com.actiontech.dble.net.service;

import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/6/15.
 */
public interface Service {

    void handle(ByteBuffer dataBuffer);

    void execute(ServiceTask task);

    void consumerInternalData();

    void cleanup();
}
