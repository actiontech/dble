package com.actiontech.dble.config.converter;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.entity.Property;
import com.actiontech.dble.cluster.zkprocess.entity.Users;
import com.actiontech.dble.cluster.zkprocess.entity.user.*;
import com.actiontech.dble.cluster.zkprocess.entity.user.privilege.Schema;
import com.actiontech.dble.cluster.zkprocess.entity.user.privilege.Table;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.loader.xml.XMLUserLoader;
import com.actiontech.dble.config.model.user.*;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.config.util.ParameterMapping;
import com.actiontech.dble.util.DecryptUtil;
import com.actiontech.dble.util.IPAddressUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UserConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserConverter.class);
    private final Gson gson;
    private final Map<UserName, UserConfig> userConfigMap = Maps.newHashMap();
    private final Map<String, Properties> blackListConfigMap = Maps.newHashMap();
    private final AtomicInteger userId = new AtomicInteger(0);

    public UserConverter() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(User.class, new UserGsonAdapter());
        this.gson = gsonBuilder.create();
    }

    public Users userJsonToBean(String userJson) {
        return ClusterLogic.parseUserJsonToBean(this.gson, userJson);
    }

    public String userBeanToJson(Users users) {
        // bean to json obj
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty(ClusterPathUtil.VERSION, users.getVersion());

        JsonArray userArray = new JsonArray();
        for (Object user : users.getUser()) {
            JsonElement tableElement = this.gson.toJsonTree(user, User.class);
            userArray.add(tableElement);
        }
        jsonObj.add(ClusterPathUtil.USER, this.gson.toJsonTree(userArray));
        jsonObj.add(ClusterPathUtil.BLACKLIST, this.gson.toJsonTree(users.getBlacklist()));
        //from json obj to string
        return this.gson.toJson(jsonObj);
    }

    public String userXmlToJson() throws JAXBException, XMLStreamException {
        XmlProcessBase xmlProcess = new XmlProcessBase();
        xmlProcess.addParseClass(Users.class);
        xmlProcess.initJaxbClass();
        String path = ClusterPathUtil.LOCAL_WRITE_PATH + ConfigFileName.USER_XML;
        String json = parseUserXmlFileToJson(xmlProcess, path);
        return json;
    }

    public void userJsonToMap(String userJson, ProblemReporter problemReporter) {
        Users users = userJsonToBean(userJson);
        List<BlackList> blacklist = Optional.ofNullable(users.getBlacklist()).orElse(Lists.newArrayList());

        Map<String, WallProvider> blackListMap;
        try {
            blackListMap = blackListToMap(blacklist, problemReporter);
            userListToMap(users.getUser(), blackListMap, problemReporter);
        } catch (Exception e) {
            throw new ConfigException("user json to map occurred  parse errors, The detailed results are as follows . " + e, e);
        }
    }

    private String parseUserXmlFileToJson(XmlProcessBase xmlParseBase, String path) throws JAXBException, XMLStreamException {
        // xml file to bean
        Users usersBean;
        try {
            usersBean = (Users) xmlParseBase.baseParseXmlToBean(path);
        } catch (Exception e) {
            LOGGER.warn("parseXmlToBean Exception", e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Xml to Shardings is :" + usersBean);
        }
        return userBeanToJson(usersBean);
    }

    private void userListToMap(List<Object> userList, Map<String, WallProvider> blackListMap, ProblemReporter problemReporter) {
        for (Object userObj : userList) {
            User user = (User) userObj;
            String name = user.getName();
            String password = user.getPassword();
            String usingDecryptStr = Optional.ofNullable(user.getUsingDecrypt()).orElse("false");
            final String whiteIPsStr = user.getWhiteIPs();
            final int maxCon = Optional.ofNullable(user.getMaxCon()).orElse(-1);
            boolean usingDecrypt;

            if (StringUtil.isEmpty(name)) {
                throw new ConfigException("one of users' name is empty");
            }
            if (StringUtil.isEmpty(password)) {
                throw new ConfigException("password of " + name + " is empty");
            }
            usingDecrypt = Boolean.parseBoolean(usingDecryptStr);
            password = DecryptUtil.decrypt(usingDecrypt, name, password);
            IPAddressUtil.checkWhiteIPs(whiteIPsStr);
            UserConfig userConfig = new UserConfig(name, password, usingDecrypt, whiteIPsStr, String.valueOf(maxCon));

            if (user instanceof ManagerUser) {
                fillManagerUser(userConfig, (ManagerUser) user);
            } else if (user instanceof ShardingUser) {
                fillShardingUser(userConfig, (ShardingUser) user, blackListMap, problemReporter);
            } else if (user instanceof RwSplitUser) {
                fillRwSplitUser(userConfig, (RwSplitUser) user, blackListMap, problemReporter);
            }
        }
    }

    private void fillRwSplitUser(UserConfig userConfig, RwSplitUser rwSplitUser, Map<String, WallProvider> blackListMap, ProblemReporter problemReporter) {
        String tenant = rwSplitUser.getTenant();
        String dbGroup = rwSplitUser.getDbGroup();
        String blacklistStr = rwSplitUser.getBlacklist();

        UserName userName = new UserName(userConfig.getName(), tenant);
        if (this.userConfigMap.containsKey(userName)) {
            throw new ConfigException("User [" + userName + "] has already existed");
        }
        if (StringUtil.isEmpty(dbGroup)) {
            throw new ConfigException("User [" + userName + "]'s dbGroup is empty");
        }

        WallProvider wallProvider = null;
        if (!StringUtil.isEmpty(blacklistStr)) {
            wallProvider = blackListMap.get(blacklistStr);
            if (wallProvider == null) {
                problemReporter.warn("blacklist[" + blacklistStr + "] for user [" + userName + "]  is not found, it will be ignore");
            } else {
                wallProvider.setName(blacklistStr);
            }
        }

        RwSplitUserConfig rwSplitUserConfig = new RwSplitUserConfig(userConfig, userName.getTenant(), wallProvider, dbGroup);
        rwSplitUserConfig.setId(this.userId.incrementAndGet());
        this.userConfigMap.put(userName, rwSplitUserConfig);
    }

    private void fillShardingUser(UserConfig userConfig, ShardingUser shardingUser, Map<String, WallProvider> blackListMap, ProblemReporter problemReporter) {
        String tenant = shardingUser.getTenant();
        final boolean readOnly = Optional.ofNullable(shardingUser.getReadOnly()).orElse(false);
        String schemas = shardingUser.getSchemas();
        String blacklistStr = shardingUser.getBlacklist();

        UserName userName = new UserName(userConfig.getName(), tenant);
        if (this.userConfigMap.containsKey(userName)) {
            throw new ConfigException("User [" + userName + "] has already existed");
        }
        if (StringUtil.isEmpty(schemas)) {
            throw new ConfigException("User [" + userName + "]'s schemas is empty");
        }
        String[] strArray = SplitUtil.split(schemas, ',', true);

        WallProvider wallProvider = null;
        if (!StringUtil.isEmpty(blacklistStr)) {
            wallProvider = blackListMap.get(blacklistStr);
            if (wallProvider == null) {
                problemReporter.warn("blacklist[" + blacklistStr + "] for user [" + userName + "]  is not found, it will be ignore");
            } else {
                wallProvider.setName(blacklistStr);
            }
        }
        // load DML Privileges
        Privileges privileges = shardingUser.getPrivileges();
        UserPrivilegesConfig privilegesConfig = loadPrivilegesConfig(privileges, userConfig);

        ShardingUserConfig shardingUserConfig = new ShardingUserConfig(userConfig, userName.getTenant(), wallProvider, readOnly, new HashSet<>(Arrays.asList(strArray)), privilegesConfig);
        shardingUserConfig.setId(this.userId.incrementAndGet());
        this.userConfigMap.put(userName, shardingUserConfig);
    }

    private UserPrivilegesConfig loadPrivilegesConfig(Privileges privileges, UserConfig userConfig) {
        UserPrivilegesConfig privilegesConfig = null;
        List<Schema> schemaList = null == privileges ? Lists.newArrayList() : Optional.ofNullable(privileges.getSchema()).orElse(Collections.EMPTY_LIST);
        boolean check = null == privileges ? false : Optional.ofNullable(privileges.getCheck()).orElse(false);

        if (!schemaList.isEmpty()) {
            privilegesConfig = new UserPrivilegesConfig();
            privilegesConfig.setCheck(check);
        }
        for (Schema schema : schemaList) {
            String schemaName = schema.getName();
            String schemaDml = schema.getDml();

            if (!XMLUserLoader.DML_PATTERN.matcher(schemaDml).matches())
                throw new ConfigException("User [" + userConfig.getName() + "]'s schema [" + schemaName + "]'s privilege's dml is not correct");
            int[] dml1Array = new int[schemaDml.length()];
            for (int offset1 = 0; offset1 < schemaDml.length(); offset1++) {
                dml1Array[offset1] = Character.getNumericValue(schemaDml.charAt(offset1));
            }
            UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = new UserPrivilegesConfig.SchemaPrivilege();
            schemaPrivilege.setDml(dml1Array);

            List<Table> tableList = schema.getTable();
            for (Table table : tableList) {
                String tableDml = table.getDml();
                UserPrivilegesConfig.TablePrivilege tablePrivilege = new UserPrivilegesConfig.TablePrivilege();

                if (!XMLUserLoader.DML_PATTERN.matcher(tableDml).matches())
                    throw new ConfigException("User [" + userConfig.getName() + "]'s schema [" + schemaName + "]'s table [" + tableDml + "]'s privilege's dml is not correct");
                int[] dml2Array = new int[tableDml.length()];
                for (int offset2 = 0; offset2 < tableDml.length(); offset2++) {
                    dml2Array[offset2] = Character.getNumericValue(tableDml.charAt(offset2));
                }
                tablePrivilege.setDml(dml2Array);
                schemaPrivilege.addTablePrivilege(table.getName(), tablePrivilege);
            }
            privilegesConfig.addSchemaPrivilege(schemaName, schemaPrivilege);
        }
        return privilegesConfig;
    }

    private void fillManagerUser(UserConfig userConfig, ManagerUser managerUser) {
        Boolean readOnly = Optional.ofNullable(managerUser.getReadOnly()).orElse(false);
        UserName userName = new UserName(userConfig.getName());
        if (this.userConfigMap.containsKey(userName)) {
            throw new ConfigException("User [name:" + userConfig.getName() + "] has already existed");
        }
        ManagerUserConfig managerUserConfig = new ManagerUserConfig(userConfig, readOnly);
        managerUserConfig.setId(this.userId.incrementAndGet());

        this.userConfigMap.put(userName, managerUserConfig);
    }

    private Map<String, WallProvider> blackListToMap(List<BlackList> blacklist, ProblemReporter problemReporter) throws InvocationTargetException, IllegalAccessException {
        Map<String, WallProvider> blackListMap = Maps.newHashMap();
        for (BlackList blackList : blacklist) {
            String name = blackList.getName();
            List<Property> propertyList = blackList.getProperty();
            if (blackListMap.containsKey(name)) {
                throw new ConfigException("blacklist[" + name + "]  has already existed");
            }

            Properties props2 = new Properties();
            Properties props = new Properties();
            propertyList.forEach(property -> props.put(property.getName(), property.getValue()));
            props2.putAll(props);
            this.blackListConfigMap.put(name, props2);

            WallConfig wallConfig = new WallConfig();
            ParameterMapping.mapping(wallConfig, props, problemReporter);
            if (props.size() > 0) {
                String[] propItem = new String[props.size()];
                props.keySet().toArray(propItem);
                throw new ConfigException("blacklist item(s) is not recognized: " + StringUtil.join(propItem, ","));
            }
            WallProvider provider = new MySqlWallProvider(wallConfig);
            provider.setBlackListEnable(true);
            blackListMap.put(name, provider);
        }
        return blackListMap;
    }

    public Map<UserName, UserConfig> getUserConfigMap() {
        return userConfigMap;
    }

    public Map<String, Properties> getBlackListConfigMap() {
        return blackListConfigMap;
    }
}
