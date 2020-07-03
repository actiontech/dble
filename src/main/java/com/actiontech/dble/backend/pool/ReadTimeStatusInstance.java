package com.actiontech.dble.backend.pool;

import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;

/**
 * Created by szf on 2020/6/29.
 */
public interface ReadTimeStatusInstance {

    boolean isReadInstance();

    boolean isDisabled();

    boolean isAutocommitSynced();

    boolean isIsolationSynced();

    boolean isAlive();

    DbInstanceConfig getConfig();

    boolean skipEvit();

    DbGroupConfig getDbGroupConfig();

}
