/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.SystemConfigLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.CheckConfigurationUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-12-01
 */
public abstract class BaseSqlHintTest {
    private static final Logger LOGGER = LogManager.getLogger(BaseSqlHintTest.class);
    public static UserName USER = new UserName("test", null);
    public static String SCHEMA_NAME = "testdb";
    static ProxyMetaManager tmManager;
    ShardingService shardingService;
    static ServerConfig serverConfig;
    static SchemaConfig schemaConfig;
    static UserConfig userConfig;
    static SystemConfig systemConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CheckConfigurationUtil.checkConfiguration();
        ClusterController.loadClusterProperties();
        //lod system properties
        SystemConfigLoader.initSystemConfig();
        systemConfig = SystemConfig.getInstance();
        initConfig();
        tmManager.createDatabase(SCHEMA_NAME);
        TableMeta tableMeta;
        tableMeta = mockTable("a");
        tmManager.addTable(SCHEMA_NAME, tableMeta, true);
        tableMeta = mockTable("b");
        tmManager.addTable(SCHEMA_NAME, tableMeta, true);
        tableMeta = mockTable("c");
        tmManager.addTable(SCHEMA_NAME, tableMeta, true);
        tableMeta = mockTable("d");
        tmManager.addTable(SCHEMA_NAME, tableMeta, true);
        tableMeta = mockTable("e");
        tmManager.addTable(SCHEMA_NAME, tableMeta, true);
    }

    @Before
    public void setUpBeforeEveryTest() throws Exception {

        initService(userConfig);
    }


    private static void initConfig() {
        serverConfig = new ServerConfig();
        DbleServer.getInstance().setConfig(serverConfig);
        schemaConfig = serverConfig.getSchemas().get(SCHEMA_NAME);
        userConfig = serverConfig.getUsers().get(USER);
        tmManager = new ProxyMetaManager();
        ProxyMeta.getInstance().setTmManager(tmManager);
    }

    @Nonnull
    protected PlanNode getPlanNode(RouteResultset rrs) {
        if (rrs == null) {
            throw new RuntimeException("rrs is null");
        }
        SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(shardingService.getSchema(), shardingService.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, shardingService.getUsrVariables(), rrs.getHintPlanInfo());
        visitor.visit(ast);
        PlanNode node = visitor.getTableNode();
        node.setSql(rrs.getStatement());
        node.setUpFields();
        PlanUtil.checkTablesPrivilege(shardingService, node, ast);
        return node;
    }

    private void initService(UserConfig userConfig) {
        final FakeConnection connection = new FakeConnection(null, null);
        final AuthPacket authPacket = new AuthPacket();
        authPacket.setDatabase(SCHEMA_NAME);
        authPacket.setCharsetIndex(SystemConfig.getInstance().getFakeMySQLVersion().startsWith("8") ? 225 : 45);
        shardingService = new ShardingService(connection, new AuthResultInfo(null, authPacket, USER, userConfig));
    }

    private static TableMeta mockTable(String tableName) {
        TableMeta tableMeta = new TableMeta(1, SCHEMA_NAME, tableName);
        tableMeta.setColumns(Lists.newArrayList(new ColumnMeta("id", "int", false)));
        return tableMeta;
    }
}
