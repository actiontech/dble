package com.actiontech.dble.net.service;

/**
 * Created by szf on 2020/6/15.
 */
public interface Service {

    void handle(ServiceTask task);

    void execute(ServiceTask task);

    void cleanup();
}
