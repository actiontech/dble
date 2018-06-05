package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2018/2/1.
 */
public class UDdlChildResponse implements UcoreXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UDdlChildResponse.class);
    private static Map<String, String> lockMap = new ConcurrentHashMap<String, String>();

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        if (configValue.getKey().split("/").length == UcorePathUtil.getDDLPath().split("/").length + 2) {
            //child change the listener is not supported
            //only response for the key /un.../d.../clu.../ddl/schema.table
            return;
        } else {
            LOGGER.debug("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
            ClusterDelayProvider.delayAfterGetDdlNotice();
            String nodeName = configValue.getKey().split("/")[4];
            String[] tableInfo = nodeName.split("\\.");
            final String schema = StringUtil.removeBackQuote(tableInfo[0]);
            final String table = StringUtil.removeBackQuote(tableInfo[1]);

            if (UKvBean.DELETE.equals(configValue.getChangeType()) || "".equals(configValue.getValue())) {
                return;
            }

            DDLInfo ddlInfo = new DDLInfo(configValue.getValue());

            if (ddlInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                return; //self node
            }

            String fullName = schema + "." + table;
            //if the start node is preparing to do the ddl
            if (ddlInfo.getStatus() == DDLInfo.DDLStatus.INIT) {
                LOGGER.debug("init of ddl " + schema + " " + table);
                try {
                    lockMap.put(fullName, ddlInfo.getFrom());
                    DbleServer.getInstance().getTmManager().addMetaLock(schema, table);
                } catch (Exception t) {
                    DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                    throw t;
                }

            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.SUCCESS && !UKvBean.DELETE.equals(configValue.getChangeType()) &&
                    lockMap.containsKey(fullName)) {
                // if the start node is done the ddl execute
                lockMap.remove(fullName);

                ClusterDelayProvider.delayBeforeUpdateMeta();
                //to judge the table is be drop
                SQLStatementParser parser = new MySqlStatementParser(ddlInfo.getSql());
                SQLStatement statement = parser.parseStatement();
                if (statement instanceof SQLDropTableStatement) {
                    DbleServer.getInstance().getTmManager().updateMetaData(ddlInfo.getSchema(), ddlInfo.getSql(),
                            DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false);
                } else {
                    //else get the lastest table meta from db
                    DbleServer.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), schema, table);
                }

                ClusterDelayProvider.delayBeforeDdlResponse();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLInstancePath(fullName), UcorePathUtil.SUCCESS);
            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.FAILED && !UKvBean.DELETE.equals(configValue.getChangeType())) {
                //if the start node executing ddl with error,just release the lock
                lockMap.remove(fullName);
                DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                ClusterDelayProvider.delayBeforeDdlResponse();
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLInstancePath(fullName), UcorePathUtil.SUCCESS);
            }
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        return;
    }

    public static Map<String, String> getLockMap() {
        return lockMap;
    }

    public static void setLockMap(Map<String, String> lockMap) {
        UDdlChildResponse.lockMap = lockMap;
    }
}
