/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2018/2/1.
 */
public class DdlChildResponse implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);
    private static Map<String, String> lockMap = new ConcurrentHashMap<String, String>();

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        if (configValue.getKey().split("/").length == ClusterPathUtil.getDDLPath().split("/").length + 2) {
            //child change the listener is not supported
            //only response for the key /un.../d.../clu.../ddl/sharding.table
            return;
        } else {
            LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
            ClusterDelayProvider.delayAfterGetDdlNotice();
            String nodeName = configValue.getKey().split("/")[4];
            String[] tableInfo = nodeName.split("\\.");
            final String schema = StringUtil.removeBackQuote(tableInfo[0]);
            final String table = StringUtil.removeBackQuote(tableInfo[1]);

            if (KvBean.DELETE.equals(configValue.getChangeType()) || "".equals(configValue.getValue())) {
                return;
            }

            DDLInfo ddlInfo = new DDLInfo(configValue.getValue());

            if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
                return; //self node
            }

            String fullName = schema + "." + table;
            //if the start node is preparing to do the ddl
            if (ddlInfo.getStatus() == DDLInfo.DDLStatus.INIT) {
                LOGGER.info("init of ddl " + schema + " " + table);
                try {
                    lockMap.put(fullName, ddlInfo.getFrom());
                    ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, ddlInfo.getSql());
                } catch (Exception t) {
                    ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
                    throw t;
                }

            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.SUCCESS && !KvBean.DELETE.equals(configValue.getChangeType()) &&
                    lockMap.containsKey(fullName)) {
                LOGGER.info("ddl execute success notice");
                // if the start node is done the ddl execute
                lockMap.remove(fullName);

                ClusterDelayProvider.delayBeforeUpdateMeta();
                //to judge the table is be drop
                if (ddlInfo.getType() == DDLInfo.DDLType.DROP_TABLE) {
                    ProxyMeta.getInstance().getTmManager().updateMetaData(schema, table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false, DDLInfo.DDLType.DROP_TABLE);
                } else {
                    //else get the lastest table meta from db
                    ProxyMeta.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), schema, table);
                }

                ClusterDelayProvider.delayBeforeDdlResponse();
                ClusterHelper.setKV(ClusterPathUtil.getDDLInstancePath(fullName), ClusterPathUtil.SUCCESS);
            } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.FAILED && !KvBean.DELETE.equals(configValue.getChangeType())) {
                LOGGER.info("ddl execute failed notice");
                //if the start node executing ddl with error,just release the lock
                lockMap.remove(fullName);
                ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
                ClusterDelayProvider.delayBeforeDdlResponse();
                ClusterHelper.setKV(ClusterPathUtil.getDDLInstancePath(fullName), ClusterPathUtil.SUCCESS);
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
        DdlChildResponse.lockMap = lockMap;
    }
}
