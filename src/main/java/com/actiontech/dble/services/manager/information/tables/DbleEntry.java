package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.loader.xml.XMLUserLoader;
import com.actiontech.dble.config.model.user.*;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.DecryptUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class DbleEntry extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_entry";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_USER_TYPE = "user_type";
    private static final String COLUMN_USERNAME = "username";
    private static final String COLUMN_PASSWORD_ENCRYPT = "password_encrypt";
    private static final String COLUMN_ENCRYPT_CONFIGURED = "encrypt_configured";
    private static final String COLUMN_CONN_ATTR_KEY = "conn_attr_key";
    private static final String COLUMN_CONN_ATTR_VALUE = "conn_attr_value";
    private static final String COLUMN_WHITE_IPS = "white_ips";
    private static final String COLUMN_READONLY = "readonly";
    private static final String COLUMN_MAX_CONN_COUNT = "max_conn_count";
    private static final String COLUMN_BLACKLIST = "blacklist";

    public DbleEntry() {
        super(TABLE_NAME, 12);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TYPE, new ColumnMeta(COLUMN_TYPE, "varchar(9)", false));
        columnsType.put(COLUMN_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_USER_TYPE, new ColumnMeta(COLUMN_USER_TYPE, "varchar(12)", false));
        columnsType.put(COLUMN_USER_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_USERNAME, new ColumnMeta(COLUMN_USERNAME, "varchar(64)", false));
        columnsType.put(COLUMN_USERNAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PASSWORD_ENCRYPT, new ColumnMeta(COLUMN_PASSWORD_ENCRYPT, "varchar(200)", false));
        columnsType.put(COLUMN_PASSWORD_ENCRYPT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ENCRYPT_CONFIGURED, new ColumnMeta(COLUMN_ENCRYPT_CONFIGURED, "varchar(5)", false));
        columnsType.put(COLUMN_ENCRYPT_CONFIGURED, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONN_ATTR_KEY, new ColumnMeta(COLUMN_CONN_ATTR_KEY, "varchar(6)", true));
        columnsType.put(COLUMN_CONN_ATTR_KEY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONN_ATTR_VALUE, new ColumnMeta(COLUMN_CONN_ATTR_VALUE, "varchar(64)", true));
        columnsType.put(COLUMN_CONN_ATTR_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_WHITE_IPS, new ColumnMeta(COLUMN_WHITE_IPS, "varchar(200)", true));
        columnsType.put(COLUMN_WHITE_IPS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_READONLY, new ColumnMeta(COLUMN_READONLY, "varchar(5)", true));
        columnsType.put(COLUMN_READONLY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MAX_CONN_COUNT, new ColumnMeta(COLUMN_MAX_CONN_COUNT, "varchar(64)", false));
        columnsType.put(COLUMN_MAX_CONN_COUNT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_BLACKLIST, new ColumnMeta(COLUMN_BLACKLIST, "varchar(64)", true));
        columnsType.put(COLUMN_BLACKLIST, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        DbleServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).
                forEach(v -> {
                    UserConfig userConfig = v.getValue();
                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                    map.put(COLUMN_ID, userConfig.getId() + "");
                    if (userConfig instanceof ManagerUserConfig) {
                        getManagerUserConfig(map, (ManagerUserConfig) userConfig);
                    } else if (userConfig instanceof ShardingUserConfig) {
                        getShardingUserConfig(map, (ShardingUserConfig) userConfig);
                    } else if (userConfig instanceof RwSplitUserConfig) {
                        getRwSplitUserConfig(map, (RwSplitUserConfig) userConfig);
                    }
                    list.add(map);
                });
        return list;
    }

    private void getManagerUserConfig(LinkedHashMap<String, String> map, ManagerUserConfig userConfig) {
        map.put(COLUMN_TYPE, "username");
        map.put(COLUMN_USER_TYPE, XMLUserLoader.TYPE_MANAGER_USER);
        map.put(COLUMN_USERNAME, userConfig.getName());
        map.put(COLUMN_PASSWORD_ENCRYPT, getPasswordEncrypt(userConfig));
        map.put(COLUMN_ENCRYPT_CONFIGURED, userConfig.isEncrypt() + "");
        map.put(COLUMN_CONN_ATTR_KEY, null);
        map.put(COLUMN_CONN_ATTR_VALUE, null);
        map.put(COLUMN_WHITE_IPS, getWhiteIps(userConfig.getWhiteIPs()));
        map.put(COLUMN_READONLY, userConfig.isReadOnly() + "");
        map.put(COLUMN_MAX_CONN_COUNT, userConfig.getMaxCon() == -1 ? "no limit" : userConfig.getMaxCon() + "");
        map.put(COLUMN_BLACKLIST, null);
    }

    private void getShardingUserConfig(LinkedHashMap<String, String> map, ShardingUserConfig userConfig) {
        map.put(COLUMN_TYPE, userConfig.getTenant() != null ? "conn_attr" : "username");
        map.put(COLUMN_USER_TYPE, XMLUserLoader.TYPE_SHARDING_USER);
        map.put(COLUMN_USERNAME, userConfig.getName());
        map.put(COLUMN_PASSWORD_ENCRYPT, getPasswordEncrypt(userConfig));
        map.put(COLUMN_ENCRYPT_CONFIGURED, userConfig.isEncrypt() + "");
        map.put(COLUMN_CONN_ATTR_KEY, userConfig.getTenant() != null ? "tenant" : null);
        map.put(COLUMN_CONN_ATTR_VALUE, userConfig.getTenant());
        map.put(COLUMN_WHITE_IPS, getWhiteIps(userConfig.getWhiteIPs()));
        map.put(COLUMN_READONLY, userConfig.isReadOnly() + "");
        map.put(COLUMN_MAX_CONN_COUNT, userConfig.getMaxCon() == -1 ? "no limit" : userConfig.getMaxCon() + "");
        map.put(COLUMN_BLACKLIST, userConfig.getBlacklist() == null ? null : userConfig.getBlacklist().getName());
    }

    private void getRwSplitUserConfig(LinkedHashMap<String, String> map, RwSplitUserConfig userConfig) {
        map.put(COLUMN_TYPE, userConfig.getTenant() != null ? "conn_attr" : "username");
        map.put(COLUMN_USER_TYPE, XMLUserLoader.TYPE_RWSPLIT_USER);
        map.put(COLUMN_USERNAME, userConfig.getName());
        map.put(COLUMN_PASSWORD_ENCRYPT, getPasswordEncrypt(userConfig));
        map.put(COLUMN_ENCRYPT_CONFIGURED, userConfig.isEncrypt() + "");
        map.put(COLUMN_CONN_ATTR_KEY, userConfig.getTenant() != null ? "tenant" : null);
        map.put(COLUMN_CONN_ATTR_VALUE, userConfig.getTenant());
        map.put(COLUMN_WHITE_IPS, getWhiteIps(userConfig.getWhiteIPs()));
        map.put(COLUMN_READONLY, "-");
        map.put(COLUMN_MAX_CONN_COUNT, userConfig.getMaxCon() == -1 ? "no limit" : userConfig.getMaxCon() + "");
        map.put(COLUMN_BLACKLIST, userConfig.getBlacklist() == null ? null : userConfig.getBlacklist().getName());
    }

    public static String getPasswordEncrypt(UserConfig userConfig) {
        try {
            return DecryptUtil.encrypt("0:" + userConfig.getName() + ":" + userConfig.getPassword());
        } catch (Exception e) {
            return "******";
        }
    }

    public static String getWhiteIps(Set<String> whiteIps) {
        if (whiteIps.isEmpty()) {
            return null;
        }
        return StringUtils.join((new ArrayList<>(whiteIps)), ",");
    }
}
