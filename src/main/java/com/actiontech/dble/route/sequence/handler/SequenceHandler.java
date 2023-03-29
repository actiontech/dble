/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.services.FrontendService;

import javax.annotation.Nullable;
import java.sql.SQLNonTransientException;
import java.util.Set;

/**
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @version 1.0
 * @time Create on 2013-12-20 15:35:53
 */
public interface SequenceHandler {

    void load(RawJson sequenceJson, Set<String> currentShardingNodes) throws ConfigException;

    default void tryLoad(RawJson sequenceJson, Set<String> currentShardingNodes) throws ConfigException {
    }

    long nextId(String prefixName, @Nullable FrontendService frontendService) throws SQLNonTransientException;

}
