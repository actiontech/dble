/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.helper.ShowDatabaseHandler;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

import java.util.Optional;
import java.util.Set;

public class AnalysisUserConfig extends SingleDbGroupUserConfig {

    public AnalysisUserConfig(UserConfig user, String tenant, WallProvider blacklist, String dbGroup) {
        super(user, tenant, blacklist, dbGroup);
    }

    public boolean equalsBaseInfo(AnalysisUserConfig analysisUserConfig) {
        return super.equalsBaseInfo(analysisUserConfig) &&
                StringUtil.equalsWithEmpty(this.dbGroup, analysisUserConfig.getDbGroup());
    }

    @Override
    public int checkSchema(String schema) {
        if (schema == null) {
            return 0;
        }
        boolean exist;
        Set<String> schemas = new ShowDatabaseHandler(DbleServer.getInstance().getConfig().getDbGroups(), "name").execute(dbGroup);
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            Optional<String> result = schemas.stream().filter(item -> StringUtil.equals(item.toLowerCase(), schema.toLowerCase())).findFirst();
            exist = result.isPresent();
        } else {
            exist = schemas.contains(schema);
        }
        return exist ? 0 : ErrorCode.ER_BAD_DB_ERROR;
    }

}
