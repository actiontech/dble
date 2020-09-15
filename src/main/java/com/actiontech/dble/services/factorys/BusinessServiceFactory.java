package com.actiontech.dble.services.factorys;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
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

    public static AbstractService getBusinessService(AuthResultInfo info, AbstractConnection connection) {
        UserConfig userConfig = info.getUserConfig();
        if (userConfig instanceof ShardingUserConfig) {
            ShardingService service = new ShardingService(connection);
            service.initFromAuthInfo(info);
            return service;
        } else if (userConfig instanceof ManagerUserConfig) {
            ManagerService service = new ManagerService(connection);
            service.initFromAuthInfo(info);
            return service;
        } else if (userConfig instanceof RwSplitUserConfig) {
            RWSplitService service = new RWSplitService(connection);
            service.initFromAuthInfo(info);
            return service;
        }
        return null;
    }


    public static AbstractService getBackendBusinessService(AuthResultInfo info, AbstractConnection connection) {
        MySQLResponseService service;
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            service = new MySQLCurrentResponseService(connection);
        } else {
            service = new MySQLResponseService(connection);
        }
        return service;
    }
}
