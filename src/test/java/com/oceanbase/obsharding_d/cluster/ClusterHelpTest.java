/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import com.oceanbase.obsharding_d.config.ConfigFileName;
import com.oceanbase.obsharding_d.config.ConfigInitializer;
import com.oceanbase.obsharding_d.config.converter.DBConverter;
import com.oceanbase.obsharding_d.config.converter.SequenceConverter;
import com.oceanbase.obsharding_d.config.converter.ShardingConverter;
import com.oceanbase.obsharding_d.config.converter.UserConverter;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.*;
import com.oceanbase.obsharding_d.config.model.user.*;
import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ClusterHelpTest {

    UserConverter userConverter = new UserConverter();
    ShardingConverter shardingConverter = new ShardingConverter();
    RawJson sequencePropsToJson = null;

    ConfigInitializer configInitializerByJson = new ConfigInitializer(userConverter.userXmlToJson(), DBConverter.dbXmlToJson(), shardingConverter.shardingXmlToJson(), sequencePropsToJson);

    ConfigInitializer configInitializerByXml = new ConfigInitializer();

    public ClusterHelpTest() throws Exception {
        if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT) {
            sequencePropsToJson = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_FILE_NAME);
        } else if (ClusterConfig.getInstance().getSequenceHandlerType() == ClusterConfig.SEQUENCE_HANDLER_MYSQL) {
            sequencePropsToJson = SequenceConverter.sequencePropsToJson(ConfigFileName.SEQUENCE_DB_FILE_NAME);
        }
    }

    @Test
    public void testShardingXml() {
        Map<ERTable, Set<ERTable>> erTableSetMapByXml = configInitializerByXml.getErRelations();
        Map<String, AbstractPartitionAlgorithm> functionMapByXml = configInitializerByXml.getFunctions();
        Map<String, SchemaConfig> schemaConfigMapByXml = configInitializerByXml.getSchemas();
        Map<String, ShardingNode> shardingNodeMapByXml = configInitializerByXml.getShardingNodes();
        Map<ERTable, Set<ERTable>> erTableSetMapByJson = configInitializerByJson.getErRelations();
        Map<String, AbstractPartitionAlgorithm> functionMapByJson = configInitializerByJson.getFunctions();
        Map<String, SchemaConfig> schemaConfigMapByJson = configInitializerByJson.getSchemas();
        Map<String, ShardingNode> shardingNodeMapByJson = configInitializerByJson.getShardingNodes();

        Assert.assertEquals(erTableSetMapByXml.size(), erTableSetMapByJson.size());
        for (Map.Entry<ERTable, Set<ERTable>> erTableSetEntry : erTableSetMapByXml.entrySet()) {
            Set<ERTable> erTableJson = erTableSetMapByJson.get(erTableSetEntry.getKey());
            Assert.assertEquals(erTableJson, erTableSetEntry.getValue());
        }

        Assert.assertEquals(functionMapByXml.size(), functionMapByJson.size());
        for (Map.Entry<String, AbstractPartitionAlgorithm> algorithmEntry : functionMapByXml.entrySet()) {
            AbstractPartitionAlgorithm algorithmJson = functionMapByJson.get(algorithmEntry.getKey());
            Assert.assertEquals(algorithmJson, algorithmEntry.getValue());
        }


        Assert.assertEquals(shardingNodeMapByXml.size(), shardingNodeMapByJson.size());
        for (Map.Entry<String, ShardingNode> shardingNodeConfigEntry : shardingNodeMapByXml.entrySet()) {
            ShardingNode shardingNodeJson = shardingNodeMapByJson.get(shardingNodeConfigEntry.getKey());
            Assert.assertTrue(shardingNodeJson.equalsBaseInfo(shardingNodeConfigEntry.getValue()));
        }

        Assert.assertEquals(schemaConfigMapByXml.size(), schemaConfigMapByJson.size());
        for (Map.Entry<String, SchemaConfig> schemaConfigEntry : schemaConfigMapByXml.entrySet()) {
            SchemaConfig schemaConfigJson = schemaConfigMapByJson.get(schemaConfigEntry.getKey());
            Assert.assertTrue(schemaConfigJson.equalsBaseInfo(schemaConfigEntry.getValue()));
            Assert.assertEquals(schemaConfigJson.getTables().size(), schemaConfigEntry.getValue().getTables().size());
            for (Map.Entry<String, BaseTableConfig> tableConfigEntry : schemaConfigJson.getTables().entrySet()) {
                Assert.assertTrue(tableConfigEntry.getValue().equalsBaseInfo(schemaConfigEntry.getValue().getTables().get(tableConfigEntry.getKey())));

                BaseTableConfig baseTableConfig = schemaConfigEntry.getValue().getTables().get(tableConfigEntry.getKey());
                if (baseTableConfig instanceof ShardingTableConfig) {
                    Assert.assertTrue(((ShardingTableConfig) tableConfigEntry.getValue()).equalsBaseInfo((ShardingTableConfig) baseTableConfig));
                } else if (baseTableConfig instanceof ChildTableConfig) {
                    Assert.assertTrue(((ChildTableConfig) tableConfigEntry.getValue()).equalsBaseInfo((ChildTableConfig) baseTableConfig));
                } else if (baseTableConfig instanceof GlobalTableConfig) {
                    Assert.assertTrue(((GlobalTableConfig) tableConfigEntry.getValue()).equalsBaseInfo((GlobalTableConfig) baseTableConfig));
                } else {
                    Assert.assertTrue(tableConfigEntry.getValue().equalsBaseInfo(baseTableConfig));
                }
            }
        }

    }

    @Test
    public void testDXml() {
        Map<String, PhysicalDbGroup> dbGroupsByXml = configInitializerByXml.getDbGroups();
        Assert.assertEquals(dbGroupsByXml.size(), configInitializerByJson.getDbGroups().size());
        for (Map.Entry<String, PhysicalDbGroup> physicalDbGroupEntry : dbGroupsByXml.entrySet()) {
            PhysicalDbGroup physicalDbGroup = configInitializerByJson.getDbGroups().get(physicalDbGroupEntry.getKey());
            Assert.assertTrue(physicalDbGroupEntry.getValue().equalsBaseInfo(physicalDbGroup));
            Assert.assertEquals(physicalDbGroupEntry.getValue().getAllDbInstanceMap().size(), physicalDbGroup.getAllDbInstanceMap().size());
            for (Map.Entry<String, PhysicalDbInstance> physicalDbInstanceEntry : physicalDbGroupEntry.getValue().getAllDbInstanceMap().entrySet()) {
                Assert.assertEquals(physicalDbInstanceEntry.getValue(), physicalDbGroup.getAllDbInstanceMap().get(physicalDbInstanceEntry.getKey()));
            }
        }


        for (Map.Entry<String, PhysicalDbGroup> physicalDbGroupEntry : configInitializerByJson.getDbGroups().entrySet()) {
            PhysicalDbGroup physicalDbGroup = dbGroupsByXml.get(physicalDbGroupEntry.getKey());
            Assert.assertTrue(physicalDbGroupEntry.getValue().equalsBaseInfo(physicalDbGroup));
            Assert.assertEquals(physicalDbGroupEntry.getValue().getAllDbInstanceMap().size(), physicalDbGroup.getAllDbInstanceMap().size());
            for (Map.Entry<String, PhysicalDbInstance> physicalDbInstanceEntry : physicalDbGroupEntry.getValue().getAllDbInstanceMap().entrySet()) {
                Assert.assertEquals(physicalDbInstanceEntry.getValue(), physicalDbGroup.getAllDbInstanceMap().get(physicalDbInstanceEntry.getKey()));
            }
        }
    }


    @Test
    public void testUserXml() {
        Map<UserName, UserConfig> users = configInitializerByXml.getUsers();
        Map<String, Properties> blacklistConfig = configInitializerByXml.getBlacklistConfig();
        Map<UserName, UserConfig> userConfigMap = configInitializerByJson.getUsers();
        Map<String, Properties> blackListConfigMap = configInitializerByJson.getBlacklistConfig();
        Assert.assertEquals(users.size(), userConfigMap.size());
        Assert.assertEquals(blacklistConfig, blackListConfigMap);

        for (Map.Entry<UserName, UserConfig> userConfigEntry : users.entrySet()) {
            UserConfig userConfig = userConfigMap.get(userConfigEntry.getKey());
            if (userConfig instanceof ShardingUserConfig) {
                Assert.assertTrue(((ShardingUserConfig) userConfigEntry.getValue()).equalsBaseInfo((ShardingUserConfig) userConfig));
            } else if (userConfig instanceof RwSplitUserConfig) {
                Assert.assertTrue(((RwSplitUserConfig) userConfigEntry.getValue()).equalsBaseInfo((RwSplitUserConfig) userConfig));
            } else if (userConfig instanceof ManagerUserConfig) {
                Assert.assertTrue(((ManagerUserConfig) userConfigEntry.getValue()).equalsBaseInfo((ManagerUserConfig) userConfig));
            } else {
                Assert.assertTrue(userConfigEntry.getValue().equalsBaseInfo(userConfig));
            }
        }
        for (Map.Entry<UserName, UserConfig> userConfigEntry : userConfigMap.entrySet()) {
            UserConfig userConfig = users.get(userConfigEntry.getKey());
            if (userConfig instanceof ShardingUserConfig) {
                Assert.assertTrue(((ShardingUserConfig) userConfigEntry.getValue()).equalsBaseInfo((ShardingUserConfig) userConfig));
            } else if (userConfig instanceof RwSplitUserConfig) {
                Assert.assertTrue(((RwSplitUserConfig) userConfigEntry.getValue()).equalsBaseInfo((RwSplitUserConfig) userConfig));
            } else if (userConfig instanceof ManagerUserConfig) {
                Assert.assertTrue(((ManagerUserConfig) userConfigEntry.getValue()).equalsBaseInfo((ManagerUserConfig) userConfig));
            } else {
                Assert.assertTrue(userConfigEntry.getValue().equalsBaseInfo(userConfig));
            }
        }

    }
}
