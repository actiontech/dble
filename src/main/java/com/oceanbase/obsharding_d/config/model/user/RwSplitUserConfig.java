/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.model.user;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.helper.ShowDatabaseHandler;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.wall.WallProvider;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class RwSplitUserConfig extends SingleDbGroupUserConfig {
    private final String dbGroup;

    public RwSplitUserConfig(UserConfig user, String tenant, WallProvider blacklist, String dbGroup) {
        super(user, tenant, blacklist, dbGroup);
        this.dbGroup = dbGroup;
    }

    public String getDbGroup() {
        return dbGroup;
    }


    public boolean equalsBaseInfo(RwSplitUserConfig rwSplitUserConfig) {
        return super.equalsBaseInfo(rwSplitUserConfig) &&
                StringUtil.equalsWithEmpty(this.dbGroup, rwSplitUserConfig.getDbGroup());
    }

    @Override
    public int checkSchema(String schema) {
        if (schema == null) {
            return 0;
        }
        boolean exist;
        Set<String> schemas = new ShowDatabaseHandler(OBsharding_DServer.getInstance().getConfig().getDbGroups(), "Database").execute(dbGroup);
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            Optional<String> result = schemas.stream().filter(item -> StringUtil.equals(item.toLowerCase(), schema.toLowerCase())).findFirst();
            exist = result.isPresent();
        } else {
            exist = schemas.contains(schema);
        }
        return exist ? 0 : ErrorCode.ER_BAD_DB_ERROR;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RwSplitUserConfig that = (RwSplitUserConfig) o;
        return Objects.equals(dbGroup, that.dbGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dbGroup);
    }
}
