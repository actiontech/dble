/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.user.SingleDbGroupUserConfig;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HintDbInstanceHandler
 *
 * @author AMGuo
 */
public final class HintDbInstanceHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HintDbInstanceHandler.class);

    private HintDbInstanceHandler() {
    }

    public static PhysicalDbInstance route(String realSQL, RWSplitService service, String hintSQLValue) throws SQLException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("route dbInstance sql hint from " + realSQL);
        }

        SingleDbGroupUserConfig rwSplitUserConfig = service.getUserConfig();
        PhysicalDbInstance dbInstance = findDbInstance(rwSplitUserConfig, hintSQLValue);
        if (null == dbInstance) {
            String msg = "can't find hint dbInstance:" + hintSQLValue + " in db_group:" + rwSplitUserConfig.getDbGroup();
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        return dbInstance;
    }

    private static PhysicalDbInstance findDbInstance(SingleDbGroupUserConfig userConfig, String dbInstanceUrl) {
        if (StringUtil.isEmpty(dbInstanceUrl)) {
            return null;
        }
        PhysicalDbGroup dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups().get(userConfig.getDbGroup());
        Set<PhysicalDbInstance> dbInstanceSet = dbGroupMap.
                getDbInstances(true).stream().
                filter(dbInstance -> StringUtil.equals(dbInstance.getConfig().getUrl().trim(), dbInstanceUrl.trim())).
                collect(Collectors.toSet());
        Optional<PhysicalDbInstance> slaveInstance = dbInstanceSet.stream().filter(instance -> instance.isReadInstance()).findFirst();
        if (slaveInstance.isPresent()) {
            return slaveInstance.get();
        } else {
            Optional<PhysicalDbInstance> masterInstance = dbInstanceSet.stream().filter(instance -> !instance.isReadInstance()).findFirst();
            return masterInstance.orElse(null);
        }
    }

}
