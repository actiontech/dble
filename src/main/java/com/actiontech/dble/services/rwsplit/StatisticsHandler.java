package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.net.service.AbstractService;

import javax.annotation.Nonnull;

public interface StatisticsHandler {

    void stringEof(byte[] data, @Nonnull AbstractService service);
}
