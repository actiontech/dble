/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess;

import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ZkDistributeLock extends DistributeLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDistributeLock.class);
    private ZkSender sender;

    public ZkDistributeLock(String path, String value, ZkSender sender) {
        this.path = path;
        this.value = value;
        this.sender = sender;
    }

    @Override
    public boolean acquire() {

        try {
            ZKUtils.createTempNode(path, value.getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (Exception e) {
            LOGGER.warn("acquire ZkDistributeLock failed ", e);
            return false;
        }

    }

    @Override
    public void release() {
        try {
            sender.cleanKV(path);
        } catch (Exception e) {
            LOGGER.warn("release ZkDistributeLock failed ", e);
        }
    }
}
