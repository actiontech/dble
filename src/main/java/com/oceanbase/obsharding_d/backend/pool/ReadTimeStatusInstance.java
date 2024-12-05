/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.pool;

import com.oceanbase.obsharding_d.config.model.db.DbGroupConfig;
import com.oceanbase.obsharding_d.config.model.db.DbInstanceConfig;

/**
 * Created by szf on 2020/6/29.
 */
public interface ReadTimeStatusInstance {

    boolean isReadInstance();

    boolean isDisabled();

    boolean isAutocommitSynced();

    boolean isIsolationSynced();

    boolean isAlive();

    boolean skipEvit();

    DbInstanceConfig getConfig();

    DbGroupConfig getDbGroupConfig();

}
