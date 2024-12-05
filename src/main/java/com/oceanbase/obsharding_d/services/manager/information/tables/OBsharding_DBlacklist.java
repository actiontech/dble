/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.AnalysisUserConfig;
import com.oceanbase.obsharding_d.config.model.user.RwSplitUserConfig;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.alibaba.druid.wall.WallProvider;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.*;

public class OBsharding_DBlacklist extends ManagerBaseTable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(OBsharding_DBlacklist.class);

    private static final String TABLE_NAME = "obsharding_d_blacklist";

    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_PROPERTY_KEY = "property_key";
    private static final String COLUMN_PROPERTY_VALUE = "property_value";
    private static final String COLUMN_USER_CONFIGURED = "user_configured";

    public OBsharding_DBlacklist() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PROPERTY_KEY, new ColumnMeta(COLUMN_PROPERTY_KEY, "varchar(64)", false, true));
        columnsType.put(COLUMN_PROPERTY_KEY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PROPERTY_VALUE, new ColumnMeta(COLUMN_PROPERTY_VALUE, "varchar(5)", false));
        columnsType.put(COLUMN_PROPERTY_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_USER_CONFIGURED, new ColumnMeta(COLUMN_USER_CONFIGURED, "varchar(5)", false));
        columnsType.put(COLUMN_USER_CONFIGURED, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList();
        Collection<UserConfig> userConfigList = OBsharding_DServer.getInstance().getConfig().getUsers().values();
        Map<String, Properties> blacklistConfig = OBsharding_DServer.getInstance().getConfig().getBlacklistConfig();
        List<String> blackListNames = new ArrayList<>();
        for (UserConfig userConfig : userConfigList) {
            WallProvider blackList = null;
            if (userConfig instanceof ShardingUserConfig) {
                blackList = ((ShardingUserConfig) userConfig).getBlacklist();
            } else if (userConfig instanceof RwSplitUserConfig) {
                blackList = ((RwSplitUserConfig) userConfig).getBlacklist();
            } else if (userConfig instanceof AnalysisUserConfig) {
                blackList = ((AnalysisUserConfig) userConfig).getBlacklist();
            }
            if (blackList != null && !blackListNames.contains(blackList.getName())) {
                blackListNames.add(blackList.getName());
                getDetailedBlackList(list, blackList.getName(), blackList.getConfig(), blacklistConfig.get(blackList.getName()));
            }
        }
        return list;
    }

    private void getDetailedBlackList(List<LinkedHashMap<String, String>> list, String name, Object object, Properties blackConfig) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(object.getClass());
            PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor pd : pds) {
                if ("noneBaseStatementAllow".equals(pd.getName()) || "inited".equals(pd.getName())) {
                    continue;
                }
                Class<?> cls = pd.getPropertyType();
                if (null != cls && (cls.equals(Boolean.TYPE) || cls.equals(Boolean.class))) {
                    LinkedHashMap<String, String> blackMap = Maps.newLinkedHashMap();
                    blackMap.put(COLUMN_NAME, name);
                    blackMap.put(COLUMN_PROPERTY_KEY, pd.getName());
                    blackMap.put(COLUMN_PROPERTY_VALUE, pd.getReadMethod().invoke(object).toString());
                    boolean isuserConfigured = blackConfig.keySet().contains(pd.getName());
                    blackMap.put(COLUMN_USER_CONFIGURED, isuserConfigured + "");
                    list.add(blackMap);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("read blacklist Exception", e);
        }
    }
}
