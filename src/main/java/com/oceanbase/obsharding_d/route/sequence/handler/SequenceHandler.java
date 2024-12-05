/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.sequence.handler;

import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.services.FrontendService;

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
