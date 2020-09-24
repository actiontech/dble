package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.loader.xml.XMLUserLoader;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.IPAddressUtil;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Maps;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DbleRwSplitEntry extends ManagerWritableTable {

    public static final String TABLE_NAME = "dble_rw_split_entry";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_TYPE = "type";
    public static final String COLUMN_USERNAME = "username";
    private static final String COLUMN_PASSWORD_ENCRYPT = "password_encrypt";
    private static final String COLUMN_ENCRYPT_CONFIGURED = "encrypt_configured";
    public static final String COLUMN_CONN_ATTR_KEY = "conn_attr_key";
    public static final String COLUMN_CONN_ATTR_VALUE = "conn_attr_value";
    private static final String COLUMN_WHITE_IPS = "white_ips";
    private static final String COLUMN_MAX_CONN_COUNT = "max_conn_count";
    private static final String COLUMN_BLACKLIST = "blacklist";
    public static final String COLUMN_DB_GROUP = "db_group";

    public DbleRwSplitEntry() {
        super(TABLE_NAME, 10);
        setNotWritableColumnSet(COLUMN_ID, COLUMN_BLACKLIST, COLUMN_TYPE);
        setLogicalPrimaryKeySet(COLUMN_USERNAME, COLUMN_CONN_ATTR_KEY, COLUMN_CONN_ATTR_VALUE);
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.USER_XML;
        this.setXmlFilePath(path);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", true, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TYPE, new ColumnMeta(COLUMN_TYPE, "varchar(9)", true));
        columnsType.put(COLUMN_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_USERNAME, new ColumnMeta(COLUMN_USERNAME, "varchar(64)", false));
        columnsType.put(COLUMN_USERNAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PASSWORD_ENCRYPT, new ColumnMeta(COLUMN_PASSWORD_ENCRYPT, "varchar(200)", false));
        columnsType.put(COLUMN_PASSWORD_ENCRYPT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ENCRYPT_CONFIGURED, new ColumnMeta(COLUMN_ENCRYPT_CONFIGURED, "varchar(5)", true, "true"));
        columnsType.put(COLUMN_ENCRYPT_CONFIGURED, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONN_ATTR_KEY, new ColumnMeta(COLUMN_CONN_ATTR_KEY, "varchar(6)", true));
        columnsType.put(COLUMN_CONN_ATTR_KEY, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONN_ATTR_VALUE, new ColumnMeta(COLUMN_CONN_ATTR_VALUE, "varchar(64)", true));
        columnsType.put(COLUMN_CONN_ATTR_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_WHITE_IPS, new ColumnMeta(COLUMN_WHITE_IPS, "varchar(200)", true));
        columnsType.put(COLUMN_WHITE_IPS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MAX_CONN_COUNT, new ColumnMeta(COLUMN_MAX_CONN_COUNT, "varchar(64)", false));
        columnsType.put(COLUMN_MAX_CONN_COUNT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_BLACKLIST, new ColumnMeta(COLUMN_BLACKLIST, "varchar(64)", true));
        columnsType.put(COLUMN_BLACKLIST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_GROUP, new ColumnMeta(COLUMN_DB_GROUP, "varchar(64)", false));
        columnsType.put(COLUMN_DB_GROUP, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        Set<String> dbGroups = DbleServer.getInstance().getConfig().getDbGroups().keySet();
        DbleServer.getInstance().getConfig().getUsers().entrySet().stream().sorted(Comparator.comparingInt(a -> a.getValue().getId())).forEach(v -> {
            UserConfig userConfig = v.getValue();
            if (userConfig instanceof RwSplitUserConfig) {
                RwSplitUserConfig rwSplitUserConfig = (RwSplitUserConfig) userConfig;
                String dbGroupName = rwSplitUserConfig.getDbGroup();
                if (dbGroups.contains(dbGroupName)) {
                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                    map.put(COLUMN_ID, userConfig.getId() + "");
                    map.put(COLUMN_TYPE, rwSplitUserConfig.getTenant() != null ? "conn_attr" : "username");
                    map.put(COLUMN_USERNAME, rwSplitUserConfig.getName());
                    map.put(COLUMN_PASSWORD_ENCRYPT, DbleEntry.getPasswordEncrypt(rwSplitUserConfig));
                    map.put(COLUMN_ENCRYPT_CONFIGURED, String.valueOf(rwSplitUserConfig.isEncrypt()));
                    map.put(COLUMN_CONN_ATTR_KEY, rwSplitUserConfig.getTenant() != null ? "tenant" : null);
                    map.put(COLUMN_CONN_ATTR_VALUE, rwSplitUserConfig.getTenant());
                    map.put(COLUMN_WHITE_IPS, DbleEntry.getWhiteIps(rwSplitUserConfig.getWhiteIPs()));
                    map.put(COLUMN_MAX_CONN_COUNT, rwSplitUserConfig.getMaxCon() == -1 ? "no limit" : rwSplitUserConfig.getMaxCon() + "");
                    map.put(COLUMN_BLACKLIST, rwSplitUserConfig.getBlacklist() == null ? null : rwSplitUserConfig.getBlacklist().getName());
                    map.put(COLUMN_DB_GROUP, dbGroupName);
                    list.add(map);
                }
            }
        });
        return list;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        for (LinkedHashMap<String, String> row : rows) {
            check(row);
        }
        //write to configuration
        List<LinkedHashMap<String, String>> tempRowList = rows.stream().map(this::transformRow).collect(Collectors.toList());
        XMLUserLoader xmlUserLoader = new XMLUserLoader();
        xmlUserLoader.insertRwSplitUser(tempRowList, getXmlFilePath());
        return rows.size();
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        check(values);
        //write to configuration
        List<LinkedHashMap<String, String>> tempRowList = affectPks.stream().map(this::transformRow).collect(Collectors.toList());
        XMLUserLoader xmlUserLoader = new XMLUserLoader();
        xmlUserLoader.updateRwSplitUser(tempRowList, transformRow(values), getXmlFilePath());
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        //write to configuration
        List<LinkedHashMap<String, String>> tempRowList = affectPks.stream().map(this::transformRow).collect(Collectors.toList());
        XMLUserLoader xmlUserLoader = new XMLUserLoader();
        xmlUserLoader.deleteRwSplitUser(tempRowList, getXmlFilePath());
        return affectPks.size();
    }


    private void check(LinkedHashMap<String, String> tempRowMap) throws SQLException {
        //check whiteIPs
        checkWhiteIPs(tempRowMap);
        //check db_group
        checkDbGroup(tempRowMap);
        //check boolean
        checkBooleanVal(tempRowMap);
    }

    private void checkBooleanVal(LinkedHashMap<String, String> tempRowMap) {
        if (tempRowMap.containsKey(COLUMN_ENCRYPT_CONFIGURED) && !StringUtil.isEmpty(tempRowMap.get(COLUMN_ENCRYPT_CONFIGURED))) {
            String encryptConfigured = tempRowMap.get(COLUMN_ENCRYPT_CONFIGURED);
            if (!StringUtil.equalsIgnoreCase(encryptConfigured, Boolean.FALSE.toString()) && !StringUtil.equalsIgnoreCase(encryptConfigured, Boolean.TRUE.toString())) {
                throw new ConfigException("Column 'encrypt_configured' values only support 'false' or 'true'.");
            }
        }
    }


    private void checkWhiteIPs(LinkedHashMap<String, String> tempRowMap) {
        if (tempRowMap.containsKey(COLUMN_WHITE_IPS) && !StringUtil.isEmpty(tempRowMap.get(COLUMN_WHITE_IPS))) {
            IPAddressUtil.checkWhiteIPs(tempRowMap.get(COLUMN_WHITE_IPS));
        }
    }

    private void checkDbGroup(LinkedHashMap<String, String> tempRowMap) throws SQLException {
        if (tempRowMap.containsKey(COLUMN_DB_GROUP)) {
            Map<String, PhysicalDbGroup> dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups();
            boolean isExist = dbGroupMap.keySet().stream().anyMatch(groupName -> StringUtil.equals(groupName, tempRowMap.get(COLUMN_DB_GROUP)));
            if (!isExist) {
                throw new SQLException("Column 'db_group' value '" + tempRowMap.get(COLUMN_DB_GROUP) + "' does not exist or not active.", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
            }
        }
    }

    private LinkedHashMap<String, String> transformRow(LinkedHashMap<String, String> map) {
        if (null == map || map.isEmpty()) {
            return null;
        }
        LinkedHashMap<String, String> xmlMap = Maps.newLinkedHashMap();
        if (null != map.get(COLUMN_USERNAME)) {
            xmlMap.put("name", map.get(COLUMN_USERNAME));
        }
        if (null != map.get(COLUMN_PASSWORD_ENCRYPT)) {
            xmlMap.put("password", map.get(COLUMN_PASSWORD_ENCRYPT));
        }
        if (null != map.get(COLUMN_WHITE_IPS)) {
            xmlMap.put("whiteIPs", map.get(COLUMN_WHITE_IPS));
        }
        if (null != map.get(COLUMN_MAX_CONN_COUNT)) {
            xmlMap.put("maxCon", map.get(COLUMN_MAX_CONN_COUNT));
        }
        if (null != map.get(COLUMN_CONN_ATTR_VALUE)) {
            xmlMap.put("tenant", map.get(COLUMN_CONN_ATTR_VALUE));
        }
        if (null != map.get(COLUMN_DB_GROUP)) {
            xmlMap.put("dbGroup", map.get(COLUMN_DB_GROUP));
        }
        if (null != map.get(COLUMN_ENCRYPT_CONFIGURED)) {
            xmlMap.put("usingDecrypt", map.get(COLUMN_ENCRYPT_CONFIGURED));
        }
        return xmlMap;
    }
}
