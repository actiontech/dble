package com.actiontech.dble.services.factorys;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.mysqlsharding.MySQLCurrentResponseService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;

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
