package com.actiontech.dble.backend.datasource.check;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.services.manager.response.CheckGlobalConsistency;
import com.actiontech.dble.singleton.ProxyMeta;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Map;


/**
 * Created by szf on 2019/12/19.
 */
public class GlobalCheckJob implements Job {
    public static final String GLOBAL_TABLE_CHECK_DEFAULT_CRON = "0 0 0 * * ?";
    public static final String GLOBAL_TABLE_CHECK_DEFAULT = "CHECKSUM";
    public static final String GLOBAL_TABLE_CHECK_COUNT = "COUNT";

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalCheckJob.class);
    private volatile GlobalTableConfig tc;
    private volatile String schema;
    private volatile CheckGlobalConsistency handler;

    //used for Quartz job
    public GlobalCheckJob() {
    }

    public GlobalCheckJob(GlobalTableConfig tc, String schema, CheckGlobalConsistency handler) {
        this.tc = tc;
        this.schema = schema;
        this.handler = handler;
    }


    public void checkGlobalTable() {
        LOGGER.info("Global check start ........." + tc.getName());
        try {
            ServerConfig config = DbleServer.getInstance().getConfig();
            if (null == ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schema, tc.getName())) {
                LOGGER.info("Global check skip because of Meta don't exist:" + tc.getName());
                if (handler != null) {
                    handler.collectResult(schema, tc.getName(), 0, 0);
                }
                return;
            }
            AbstractConsistencyChecker checker;
            switch (tc.getCheckClass()) {
                case GLOBAL_TABLE_CHECK_DEFAULT:
                    checker = new CheckSumChecker();
                    break;
                case GLOBAL_TABLE_CHECK_COUNT:
                    checker = new CountChecker();
                    break;
                default:
                    final Class<?> clz = Class.forName(tc.getCheckClass());
                    checker = (AbstractConsistencyChecker) clz.newInstance();
            }
            checker.setSchema(schema);
            checker.setTableName(tc.getName());
            checker.setHandler(handler);
            for (String nodeName : tc.getShardingNodes()) {
                Map<String, ShardingNode> map = config.getShardingNodes();
                for (ShardingNode dbNode : map.values()) {
                    if (nodeName.equals(dbNode.getName())) {
                        checker.addCheckNode(dbNode.getDatabase(), dbNode);
                    }
                }
            }
            checker.startCheckTable();
        } catch (SQLNonTransientException e) {
            LOGGER.info("Global check skip because of DDL ........." + tc.getName());
        } catch (Exception e) {
            LOGGER.info("Global check error with Exception ", e);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        tc = (GlobalTableConfig) context.getJobDetail().getJobDataMap().get("tableConfig");
        schema = (String) context.getJobDetail().getJobDataMap().get("schema");
        //handler = null;
        this.checkGlobalTable();
    }
}
