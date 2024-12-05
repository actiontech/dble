/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLockManager;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.DDLInfo;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import static com.oceanbase.obsharding_d.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

/**
 * @author mycat
 */
public final class KillDdlLock {
    private KillDdlLock() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KillDdlLock.class);

    public static void response(String stmt, String tableInfo, ManagerService service) {
        LOGGER.info("execute kill ddl lock:'" + stmt + "'");
        Matcher matcher = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!matcher.matches()) {
            service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            return;
        }
        String schema = StringUtil.removeAllApostrophe(matcher.group(1));
        String table = StringUtil.removeAllApostrophe(matcher.group(5));

        boolean isRemoved;
        final ReentrantLock metaLock = ProxyMeta.getInstance().getTmManager().getMetaLock();
        metaLock.lock();
        try {
            isRemoved = ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
            // release distributed lock
            if (ClusterConfig.getInstance().isClusterEnable()) {
                String fullName = StringUtil.getUFullName(schema, table);
                for (DDLInfo.NodeStatus nodeStatus : DDLInfo.NodeStatus.values()) {
                    ClusterHelper.cleanPath(ClusterPathUtil.getDDLPath(fullName, nodeStatus));
                }
                DistributeLockManager.releaseLock(ClusterPathUtil.getDDLLockPath(fullName));
            }
        } finally {
            metaLock.unlock();
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        if (isRemoved) {
            packet.setMessage(("ddl lock is removed successfully!").getBytes());
        } else {
            packet.setMessage(("There is no ddl lock!").getBytes());
        }
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }

}
