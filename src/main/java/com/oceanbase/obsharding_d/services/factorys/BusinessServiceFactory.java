/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.factorys;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.user.*;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLCurrentResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;

/**
 * Created by szf on 2020/6/28.
 */
public final class BusinessServiceFactory {

    private BusinessServiceFactory() {
    }

    public static FrontendService<? extends UserConfig> getBusinessService(AuthResultInfo info, AbstractConnection connection) {
        UserConfig userConfig = info.getUserConfig();
        if (userConfig instanceof ShardingUserConfig) {
            return new ShardingService(connection, info);
        } else if (userConfig instanceof ManagerUserConfig) {
            return new ManagerService(connection, info);
        } else if (userConfig instanceof RwSplitUserConfig) {
            return new RWSplitService(connection, info);
        } else if (userConfig instanceof AnalysisUserConfig) {
            //it currently shares a set of logic with RwSplitUser
            return new RWSplitService(connection, info);
        }
        return null;
    }

    public static AbstractService getBackendBusinessService(AuthResultInfo info, AbstractConnection connection) {
        MySQLResponseService service;
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            service = new MySQLCurrentResponseService((BackendConnection) connection);
        } else {
            service = new MySQLResponseService((BackendConnection) connection);
        }
        return service;
    }
}
