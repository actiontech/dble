/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLockManager;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

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
                ClusterHelper.cleanPath(ClusterPathUtil.getDDLPath(fullName));
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
