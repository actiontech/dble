package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.user.RwSplitUser;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.*;
import com.google.common.collect.Maps;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
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
    public static final String CONSTANT_NO_LIMIT = "no limit";

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
                    map.put(COLUMN_MAX_CONN_COUNT, rwSplitUserConfig.getMaxCon() == 0 ? CONSTANT_NO_LIMIT : rwSplitUserConfig.getMaxCon() + "");
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
        List<RwSplitUser> rwSplitUserList = rows.stream().map(this::transformRowToUser).collect(Collectors.toList());
        Users users = getUser();

        checkLogicalUniqueKeyDuplicate(users, rwSplitUserList);

        users.getUser().addAll(rwSplitUserList);

        saveUsers(users, getXmlFilePath());
        return rows.size();
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        affectPks.forEach(affectPk -> {
            if (Boolean.FALSE.toString().equalsIgnoreCase(affectPk.get(COLUMN_ENCRYPT_CONFIGURED))) {
                String password = DecryptUtil.decrypt(true, affectPk.get(COLUMN_USERNAME), affectPk.get(COLUMN_PASSWORD_ENCRYPT));
                affectPk.put(COLUMN_PASSWORD_ENCRYPT, password);
            }
            affectPk.putAll(values);
        });
        List<RwSplitUser> rwSplitUserList = affectPks.stream().map(this::transformRowToUser).collect(Collectors.toList());
        Users users = getUser();
        updateList(users, rwSplitUserList, false);

        saveUsers(users, getXmlFilePath());
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        List<RwSplitUser> rwSplitUserList = affectPks.stream().map(this::transformRowToUser).collect(Collectors.toList());
        Users users = getUser();
        updateList(users, rwSplitUserList, true);

        saveUsers(users, getXmlFilePath());
        return affectPks.size();
    }


    private void saveUsers(Users users, String xmlFilePath) {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Users.class);
        try {
            xmlProcess.initJaxbClass();
        } catch (JAXBException e) {
            throw new ConfigException(e);
        }
        xmlProcess.writeObjToXml(users, xmlFilePath, "user");
    }
    private Users getUser() {
        XmlProcessBase xmlParseBase = new XmlProcessBase();
        // xml file to bean
        Users usersBean = null;
        try {
            xmlParseBase.addParseClass(Users.class);
            xmlParseBase.initJaxbClass();
            usersBean = (Users) xmlParseBase.baseParseXmlToBean(ConfigFileName.USER_XML);
        } catch (JAXBException | XMLStreamException e) {
            e.printStackTrace();
        }
        if (null == usersBean) {
            throw new ConfigException("configuration is empty");
        }
        return usersBean;
    }

    private void updateList(Users users, List<RwSplitUser> rwSplitUserList, boolean isDelete) {
        for (RwSplitUser rwSplitUser : rwSplitUserList) {
            for (int i = 0; i < users.getUser().size(); i++) {
                Object obj = users.getUser().get(i);
                if (obj instanceof RwSplitUser) {
                    RwSplitUser sourceUser = (RwSplitUser) obj;
                    if (StringUtil.equals(sourceUser.getName(), rwSplitUser.getName()) && StringUtil.equals(sourceUser.getTenant(), rwSplitUser.getTenant())) {
                        if (!isDelete) {
                            users.getUser().set(i, rwSplitUser);
                        } else {
                            users.getUser().remove(i);
                        }
                        break;
                    }
                }
            }
        }
    }

    private void checkLogicalUniqueKeyDuplicate(Users users, List<RwSplitUser> rwSplitUserList) throws SQLException {
        List<RwSplitUser> sourceList = users.getUser().stream().filter(user -> user instanceof RwSplitUser).map(user -> (RwSplitUser) user).collect(Collectors.toList());
        for (RwSplitUser rwSplitUser : rwSplitUserList) {
            boolean isExist = sourceList.stream().anyMatch(sourceUser -> StringUtil.equals(sourceUser.getName(), rwSplitUser.getName()) && StringUtil.equals(sourceUser.getTenant(), rwSplitUser.getTenant()));
            if (isExist) {
                String msg = String.format("Duplicate entry '%s-%s-%s'for logical unique '%s-%s-%s'", rwSplitUser.getName(),
                        StringUtil.isEmpty(rwSplitUser.getTenant()) ? null : "tenant", rwSplitUser.getTenant(), COLUMN_USERNAME, COLUMN_CONN_ATTR_KEY, COLUMN_CONN_ATTR_VALUE);
                throw new SQLException(msg, "42S22", ErrorCode.ER_DUP_ENTRY);
            }
        }
    }


    private void checkBooleanVal(LinkedHashMap<String, String> tempRowMap) {
        if (tempRowMap.containsKey(COLUMN_ENCRYPT_CONFIGURED) && !StringUtil.isEmpty(tempRowMap.get(COLUMN_ENCRYPT_CONFIGURED))) {
            String encryptConfigured = tempRowMap.get(COLUMN_ENCRYPT_CONFIGURED);
            if (!StringUtil.equalsIgnoreCase(encryptConfigured, Boolean.FALSE.toString()) && !StringUtil.equalsIgnoreCase(encryptConfigured, Boolean.TRUE.toString())) {
                throw new ConfigException("Column 'encrypt_configured' values only support 'false' or 'true'.");
            }
        }
        if (!StringUtil.isBlank(tempRowMap.get(COLUMN_CONN_ATTR_KEY))) {
            if (!StringUtil.equals(tempRowMap.get(COLUMN_CONN_ATTR_KEY), "tenant")) {
                throw new ConfigException("'conn_attr_key' value is ['tenant',null].");
            }
            if (StringUtil.isBlank(tempRowMap.get(COLUMN_CONN_ATTR_VALUE))) {
                throw new ConfigException("'conn_attr_key' and 'conn_attr_value' are used together.");
            }
        } else {
            if (!StringUtil.isBlank(tempRowMap.get(COLUMN_CONN_ATTR_VALUE))) {
                throw new ConfigException("'conn_attr_key' and 'conn_attr_value' are used together.");
            }
        }
    }

    private RwSplitUser transformRowToUser(LinkedHashMap<String, String> map) {
        if (null == map || map.isEmpty()) {
            return null;
        }
        check(map);
        RwSplitUser rwSplitUser = new RwSplitUser();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case COLUMN_USERNAME:
                    rwSplitUser.setName(entry.getValue());
                    break;
                case COLUMN_PASSWORD_ENCRYPT:
                    rwSplitUser.setPassword(entry.getValue());
                    break;
                case COLUMN_ENCRYPT_CONFIGURED:
                    rwSplitUser.setUsingDecrypt(entry.getValue());
                    break;
                case COLUMN_CONN_ATTR_VALUE:
                    rwSplitUser.setTenant(entry.getValue());
                    break;
                case COLUMN_WHITE_IPS:
                    rwSplitUser.setWhiteIPs(entry.getValue());
                    break;
                case COLUMN_MAX_CONN_COUNT:
                    if (!StringUtil.isBlank(entry.getValue())) {
                        rwSplitUser.setMaxCon(IntegerUtil.parseInt(entry.getValue().replace(CONSTANT_NO_LIMIT, "0")));
                    }
                    if (rwSplitUser.getMaxCon() < 0) {
                        throw new ConfigException("Column 'max_conn_count' value cannot be less than 0.");
                    }
                    break;
                case COLUMN_DB_GROUP:
                    rwSplitUser.setDbGroup(entry.getValue());
                    break;
                default:
                    break;
            }
        }
        return rwSplitUser;
    }

    private void check(LinkedHashMap<String, String> tempRowMap) {
        //check whiteIPs
        checkWhiteIPs(tempRowMap);
        //check db_group
        checkDbGroup(tempRowMap);
        //check boolean
        checkBooleanVal(tempRowMap);
    }

    private void checkDbGroup(LinkedHashMap<String, String> tempRowMap) {
        if (tempRowMap.containsKey(COLUMN_DB_GROUP)) {
            Map<String, PhysicalDbGroup> dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups();
            boolean isExist = dbGroupMap.keySet().stream().anyMatch(groupName -> StringUtil.equals(groupName, tempRowMap.get(COLUMN_DB_GROUP)));
            if (!isExist) {
                throw new ConfigException("Column 'db_group' value '" + tempRowMap.get(COLUMN_DB_GROUP) + "' does not exist or not active.");
            }
        }
    }


    private void checkWhiteIPs(LinkedHashMap<String, String> tempRowMap) {
        if (tempRowMap.containsKey(COLUMN_WHITE_IPS) && !StringUtil.isEmpty(tempRowMap.get(COLUMN_WHITE_IPS))) {
            IPAddressUtil.checkWhiteIPs(tempRowMap.get(COLUMN_WHITE_IPS));
        }
    }
}
