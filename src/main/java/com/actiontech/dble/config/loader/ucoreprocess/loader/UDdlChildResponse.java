package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
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
        LOGGER.debug("get into " + configValue.getValue());
        if (configValue.getKey().split("/").length == 6) {
            //child change the listener is not supported
            return;
        } else {
            String nodeName = configValue.getKey().split("/")[4];
            String[] tableInfo = nodeName.split("\\.");
            final String schema = StringUtil.removeBackQuote(tableInfo[0]);
            final String table = StringUtil.removeBackQuote(tableInfo[1]);

            if ("".equals(configValue.getValue())) {
                //if the value is "" means the the ddl start server is shutdown after the ddl notify
                DbleServer.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), schema, table);
            }
            DDLInfo ddlInfo = new DDLInfo(configValue.getValue());

            if (ddlInfo.getFrom().equals(UcoreConfig.getInstance().getValue(UcoreParamCfg.UCORE_CFG_MYID))) {
                return; //self node
            }

            if (ddlInfo.getStatus() == DDLInfo.DDLStatus.INIT) {
                try {
                    LOGGER.debug("DDL LOCK SUCCESS IN " + configValue.getValue());
                    lockMap.put(schema + "." + table, ddlInfo.getFrom());
                    DbleServer.getInstance().getTmManager().addMetaLock(schema, table);
                } catch (Exception t) {
                    DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                    throw t;
                }
            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.SUCCESS && configValue.getChangeType() != UKvBean.DELETE &&
                    lockMap.containsKey(schema + "." + table)) {
                LOGGER.debug("DDL EXECUTE SUCCESS IN " + configValue.getValue());
                lockMap.remove(schema + "." + table);
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
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLInstancePath(schema + "." + table), "SUCCESS");
            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.FAILED && configValue.getChangeType() != UKvBean.DELETE) {
                lockMap.remove(schema + "." + table);
                DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getDDLInstancePath(schema + "." + table), "FAILED");
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

    @Override
    public void notifyProcessWithKey(String key, String value) throws Exception {
        return;
    }
}
